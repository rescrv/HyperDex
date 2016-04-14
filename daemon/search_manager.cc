// Copyright (c) 2011, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#define __STDC_LIMIT_MACROS

// STL
#include <algorithm>
#include <sstream>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/time.h>

// e
#include <e/intrusive_ptr.h>
#include <e/endian.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/search_manager.h"

using hyperdex::datatype_info;
using hyperdex::search_manager;
using hyperdex::reconfigure_returncode;

/////////////////////////////// Search Manager ID //////////////////////////////

class search_manager::id
{
public:
	id(const region_id re, const server_id cl, uint64_t sid);
	~id() throw ();

public:
	bool operator < (const id &rhs) const;
	bool operator == (const id &rhs) const;

public:
	region_id region;
	server_id client;
	uint64_t search_id;
};

search_manager :: id :: id(const region_id re,
                           const server_id cl,
                           uint64_t sid)
	: region(re)
	, client(cl)
	, search_id(sid)
{
}

search_manager :: id :: ~id() throw ()
{
}

bool
search_manager :: id :: operator < (const id &rhs) const
{
	if (region == rhs.region)
	{
		if (client == rhs.client)
		{
			return search_id < rhs.search_id;
		}
		return client < rhs.client;
	}
	return region < rhs.region;
}

bool
search_manager :: id :: operator == (const id &rhs) const
{
	return region == rhs.region &&
	       client == rhs.client &&
	       search_id == rhs.search_id;
}

///////////////////////////// Search Manager State /////////////////////////////

class search_manager::state
{
public:
	state(const region_id &region,
	      std::auto_ptr<e::buffer> msg,
	      std::vector<attribute_check> *checks);
	~state() throw ();

public:
	po6::threads::mutex lock;
	const region_id region;
	const std::auto_ptr<e::buffer> backing;
	std::vector<attribute_check> checks;
	e::intrusive_ptr<datalayer::iterator> iter;

private:
	friend class e::intrusive_ptr<state>;

private:
	void inc() { __sync_add_and_fetch(&m_ref, 1); }
	void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

private:
	size_t m_ref;
};

search_manager :: state :: state(const region_id &r,
                                 std::auto_ptr<e::buffer> msg,
                                 std::vector<attribute_check> *c)
	: lock()
	, region(r)
	, backing(msg)
	, checks()
	, iter()
	, m_ref(0)
{
	checks.swap(*c);
}

search_manager :: state :: ~state() throw ()
{
}

//////////////////////////////// Search Manager ////////////////////////////////

search_manager :: search_manager(daemon *d)
	: m_daemon(d)
	, m_searches(10)
{
}

search_manager :: ~search_manager() throw ()
{
}

bool
search_manager :: setup()
{
	return true;
}

void
search_manager :: teardown()
{
}

void
search_manager :: pause()
{
}

void
search_manager :: unpause()
{
}

void
search_manager :: reconfigure(const configuration &,
                              const configuration &,
                              const server_id &)
{
	// XXX cleanup dead or old searches
}

void
search_manager :: start(const server_id &from,
                        const virtual_server_id &to,
                        std::auto_ptr<e::buffer> msg,
                        uint64_t nonce,
                        uint64_t search_id,
                        std::vector<attribute_check> *checks)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	id sid(ri, from, search_id);
	if (m_searches.contains(sid))
	{
		LOG(WARNING) << "received request for search " << search_id << " from client "
		             << from << " but the search is already in progress";
		return;
	}
	e::intrusive_ptr<state> st = new state(ri, msg, checks);
	std::stable_sort(st->checks.begin(), st->checks.end());
	datalayer::returncode rc = datalayer::SUCCESS;
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	st->iter = m_daemon->m_data.make_search_iterator(snap, ri, st->checks, NULL);
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		return;
	default:
		abort();
	}
	m_searches.insert(sid, st);
	next(from, to, nonce, search_id);
}

void
search_manager :: next(const server_id &from,
                       const virtual_server_id &to,
                       uint64_t nonce,
                       uint64_t search_id)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema &sc(*m_daemon->m_config.get_schema(ri));
	id sid(ri, from, search_id);
	e::intrusive_ptr<state> st;
	if (!m_searches.lookup(sid, &st))
	{
		std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERDEX_HEADER_SIZE_VC + sizeof(uint64_t)));
		msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce;
		m_daemon->m_comm.send_client(to, from, RESP_SEARCH_DONE, msg);
		return;
	}
	po6::threads::mutex::hold hold(&st->lock);
	if (st->iter->valid())
	{
		e::slice key;
		std::vector<e::slice> val;
		uint64_t ver;
		datalayer::reference tmp;
		m_daemon->m_data.get_from_iterator(ri, sc, st->iter.get(), &key, &val, &ver, &tmp);
		size_t sz = HYPERDEX_HEADER_SIZE_VC
		            + sizeof(uint64_t)
		            + pack_size(key)
		            + pack_size(val);
		std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
		msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce << key << val;
		m_daemon->m_comm.send_client(to, from, RESP_SEARCH_ITEM, msg);
		st->iter->next();
	}
	else
	{
		std::auto_ptr<e::buffer> msg(e::buffer::create(HYPERDEX_HEADER_SIZE_VC + sizeof(uint64_t)));
		msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce;
		m_daemon->m_comm.send_client(to, from, RESP_SEARCH_DONE, msg);
		stop(from, to, search_id);
	}
}

void
search_manager :: stop(const server_id &from,
                       const virtual_server_id &to,
                       uint64_t search_id)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	id sid(ri, from, search_id);
	m_searches.remove(sid);
}

namespace hyperdex
{

struct _sorted_search_params
{
	_sorted_search_params(const schema *_sc,
	                      uint16_t _sort_by,
	                      bool _maximize)
		: sc(_sc), sort_by(_sort_by), maximize(_maximize) {}
	~_sorted_search_params() throw () {}
	const schema *sc;
	uint16_t sort_by;
	bool maximize;

private:
	_sorted_search_params(const _sorted_search_params &);
	_sorted_search_params &operator = (const _sorted_search_params &);
};

struct _sorted_search_item
{
	_sorted_search_item(_sorted_search_params *p)
		: params(p), key(), value(), version(), ref() {}
	_sorted_search_item(_sorted_search_params *p, uint16_t val_sz)
		: params(p), key_str(), version(), ref()
	{
		value.reserve(val_sz);
		value_str.reserve(val_sz);
	}
	_sorted_search_item(const _sorted_search_item &other);
	~_sorted_search_item() throw () {}
	_sorted_search_item &operator = (const _sorted_search_item &other);
	void assign();
	_sorted_search_params *params;
	std::string key_str;
	e::slice key;
	std::vector<std::string> value_str;
	std::vector<e::slice> value;
	uint64_t version;
	datalayer::reference ref;
};

_sorted_search_item :: _sorted_search_item(const _sorted_search_item &other)
	: params(other.params)
	, key_str(other.key_str)
	, key(other.key)
	, value_str(other.value_str)
	, value(other.value)
	, version(other.version)
	, ref(other.ref)
{
}

_sorted_search_item &
_sorted_search_item :: _sorted_search_item :: operator = (const _sorted_search_item &other)
{
	params = other.params;
	key_str = other.key_str;
	key = other.key;
	value_str = other.value_str;
	value = other.value;
	version = other.version;
	ref = other.ref;
	return *this;
}

void
_sorted_search_item :: assign ()
{
	key = e::slice(key_str);
	value.clear();
	for (size_t i = 0; i < value_str.size(); i ++)
	{
		value.push_back(e::slice(value_str[i]));
	}
}

bool
operator < (const _sorted_search_item &lhs, const _sorted_search_item &rhs)
{
	assert(lhs.params == rhs.params);
	_sorted_search_params *params = lhs.params;
	if (params->sort_by >= params->sc->attrs_sz)
	{
		return false;
	}
	int cmp = 0;
	if (params->sort_by == 0)
	{
		datatype_info *di = datatype_info::lookup(params->sc->attrs[0].type);
		e::slice lhs_key(lhs.key_str);
		e::slice rhs_key(rhs.key_str);
		cmp = di->compare(lhs_key, rhs_key);
	}
	else
	{
		datatype_info *di = datatype_info::lookup(params->sc->attrs[params->sort_by].type);
		e::slice lhs_value(lhs.value_str[params->sort_by - 1]);
		e::slice rhs_value(rhs.value_str[params->sort_by - 1]);
		cmp = di->compare(lhs_value, rhs_value);
	}
	if (params->maximize)
	{
		return cmp < 0;
	}
	else
	{
		return cmp > 0;
	}
}

bool
operator > (const _sorted_search_item &lhs, const _sorted_search_item &rhs)
{
	assert(lhs.params == rhs.params);
	_sorted_search_params *params = lhs.params;
	if (params->sort_by >= params->sc->attrs_sz)
	{
		return false;
	}
	int cmp = 0;
	if (params->sort_by == 0)
	{
		datatype_info *di = datatype_info::lookup(params->sc->attrs[0].type);
		e::slice lhs_key(lhs.key_str);
		e::slice rhs_key(rhs.key_str);
		cmp = di->compare(lhs_key, rhs_key);
	}
	else
	{
		datatype_info *di = datatype_info::lookup(params->sc->attrs[params->sort_by].type);
		e::slice lhs_value(lhs.value_str[params->sort_by - 1]);
		e::slice rhs_value(rhs.value_str[params->sort_by - 1]);
		cmp = di->compare(lhs_value, rhs_value);
	}
	if (params->maximize)
	{
		return cmp > 0;
	}
	else
	{
		return cmp < 0;
	}
}

} // namespace hyperdex

void
search_manager :: sorted_search(const server_id &from,
                                const virtual_server_id &to,
                                uint64_t nonce,
                                std::vector<attribute_check> *checks,
                                uint64_t limit,
                                uint16_t sort_by,
                                bool maximize)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	std::stable_sort(checks->begin(), checks->end());
	datalayer::returncode rc = datalayer::SUCCESS;
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	e::intrusive_ptr<datalayer::iterator> iter;
	iter = m_daemon->m_data.make_search_iterator(snap, ri, *checks, NULL);
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		break;
	default:
		abort();
	}
	_sorted_search_params params(sc, sort_by, maximize);
	std::vector<_sorted_search_item> top_n;
	top_n.reserve(limit + 1);
	while (iter->valid())
	{
		e::slice key;
		std::vector<e::slice> val;
		uint64_t ver;
		datalayer::reference tmp;
		m_daemon->m_data.get_from_iterator(ri, *sc, iter.get(), &key, &val, &ver, &tmp);
		_sorted_search_item ssi(&params, sc->attrs_sz);
		ssi.key_str = key.str();
		for (size_t i = 0; i < val.size(); i ++)
		{
			ssi.value_str.push_back(val[i].str());
		}
		top_n.push_back(ssi);
		std::push_heap(top_n.begin(), top_n.end());
		if (top_n.size() > limit)
		{
			std::pop_heap(top_n.begin(), top_n.end());
			top_n.pop_back();
		}
		iter->next();
	}
	std::sort(top_n.begin(), top_n.end(), std::greater<_sorted_search_item>());
	size_t sz = HYPERDEX_HEADER_SIZE_VC + sizeof(uint64_t) + sizeof(uint64_t);
	for (size_t i = 0; i < top_n.size(); ++i)
	{
		top_n[i].assign();
		sz += pack_size(top_n[i].key) + pack_size(top_n[i].value);
	}
	std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
	e::packer pa = msg->pack_at(HYPERDEX_HEADER_SIZE_VC);
	pa = pa << nonce << static_cast<uint64_t>(top_n.size());
	for (size_t i = 0; i < top_n.size(); ++i)
	{
		pa = pa << top_n[i].key << top_n[i].value;
	}
	m_daemon->m_comm.send_client(to, from, RESP_SORTED_SEARCH, msg);
}

void
search_manager :: group_keyop(const server_id &from,
                              const virtual_server_id &to,
                              uint64_t nonce,
                              std::vector<attribute_check> *checks,
                              network_msgtype mt,
                              const e::slice &remain,
                              network_msgtype resp)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	std::stable_sort(checks->begin(), checks->end());
	datalayer::returncode rc = datalayer::SUCCESS;
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	e::intrusive_ptr<datalayer::iterator> iter;
	iter = m_daemon->m_data.make_search_iterator(snap, ri, *checks, NULL);
	uint64_t result = 0;
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		result = UINT64_MAX;
		break;
	default:
		abort();
	}
	while (iter->valid() && result < UINT64_MAX)
	{
		e::slice key;
		std::vector<e::slice> val;
		uint64_t ver;
		datalayer::reference tmp;
		m_daemon->m_data.get_from_iterator(ri, *sc, iter.get(), &key, &val, &ver, &tmp);
		size_t sz = HYPERDEX_HEADER_SIZE_SV // SV because we imitate a client
		            + sizeof(uint64_t)
		            + pack_size(key)
		            + remain.size();
		std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
		msg->pack_at(HYPERDEX_HEADER_SIZE_SV)
		        << uint64_t(0) << key << e::pack_memmove(remain.data(), remain.size());
		virtual_server_id vsi = m_daemon->m_config.point_leader(ri, key);
		if (vsi != virtual_server_id())
		{
			m_daemon->m_comm.send(vsi, mt, msg);
			result++;
		}
		iter->next();
	}
	size_t sz = HYPERDEX_HEADER_SIZE_VC
	            + sizeof(uint64_t)
	            + sizeof(uint64_t);
	std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
	msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce << result;
	m_daemon->m_comm.send_client(to, from, resp, msg);
}

void
search_manager :: count(const server_id &from,
                        const virtual_server_id &to,
                        uint64_t nonce,
                        std::vector<attribute_check> *checks)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	std::stable_sort(checks->begin(), checks->end());
	datalayer::returncode rc = datalayer::SUCCESS;
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	e::intrusive_ptr<datalayer::iterator> iter;
	iter = m_daemon->m_data.make_search_iterator(snap, ri, *checks, NULL);
	uint64_t result = 0;
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		result = UINT64_MAX;
		break;
	default:
		abort();
	}
	while (iter->valid() && result < UINT64_MAX)
	{
		++result;
		iter->next();
	}
	size_t sz = HYPERDEX_HEADER_SIZE_VC
	            + sizeof(uint64_t)
	            + sizeof(uint64_t);
	std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
	msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce << result;
	m_daemon->m_comm.send_client(to, from, RESP_COUNT, msg);
}

void
search_manager :: sum(const server_id &from,
                      const virtual_server_id &to,
                      uint64_t nonce,
                      std::vector<attribute_check> *checks,
                      uint16_t sum_idx)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	std::stable_sort(checks->begin(), checks->end());
	datalayer::returncode rc = datalayer::SUCCESS;
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	e::intrusive_ptr<datalayer::iterator> iter;
	iter = m_daemon->m_data.make_search_iterator(snap, ri, *checks, NULL);
	uint64_t result = 0;
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		result = UINT64_MAX;
		break;
	default:
		abort();
	}
	while (iter->valid() && result < UINT64_MAX)
	{
		e::slice key;
		std::vector<e::slice> val;
		uint64_t ver;
		datalayer::reference tmp;
		m_daemon->m_data.get_from_iterator(ri, *sc, iter.get(), &key, &val, &ver, &tmp);
		if (val.size() < sum_idx)
		{
			LOG(ERROR) << "occur some error for sum:  " << val.size();
		}
		switch (sc->attrs[sum_idx].type)
		{
		case HYPERDATATYPE_INT64:
		{
			uint64_t num = 0;
			e::unpack64le(val[sum_idx - 1].cdata(), &num);
			result += num;
		}
		break;
		case HYPERDATATYPE_FLOAT:
		{
			double num = 0;
			e::unpackdoublele(val[sum_idx - 1].cdata(), &num);
			result += (uint64_t)num;
		}
		break;
		case HYPERDATATYPE_STRING:
		default:
			LOG(ERROR) << "could not sum item because of not int and double(" << sc->attrs[sum_idx].type << ")";
		}
		iter->next();
	}
	//LOG(INFO) << "result(" << result << ")";
	size_t sz = HYPERDEX_HEADER_SIZE_VC
	            + sizeof(uint64_t)
	            + sizeof(uint64_t);
	std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
	msg->pack_at(HYPERDEX_HEADER_SIZE_VC) << nonce << result;
	m_daemon->m_comm.send_client(to, from, RESP_SUM, msg);
}

void
search_manager :: search_describe(const server_id &from,
                                  const virtual_server_id &to,
                                  uint64_t nonce,
                                  std::vector<attribute_check> *checks)
{
	region_id ri(m_daemon->m_config.get_region_id(to));
	const schema *sc = m_daemon->m_config.get_schema(ri);
	if (sc->authorization)
	{
		return;
	}
	std::stable_sort(checks->begin(), checks->end());
	datalayer::returncode rc = datalayer::SUCCESS;
	std::ostringstream ostr;
	ostr << "search\n";
	uint64_t t_start = po6::monotonic_time();
	datalayer::snapshot snap = m_daemon->m_data.make_snapshot();
	uint64_t t_end = po6::monotonic_time();
	ostr << " snapshot took " << t_end - t_start << "ns\n";
	e::intrusive_ptr<datalayer::iterator> iter;
	t_start = po6::monotonic_time();
	iter = m_daemon->m_data.make_search_iterator(snap, ri, *checks, &ostr);
	t_end = po6::monotonic_time();
	ostr << " iterator took " << t_end - t_start << "ns\n";
	switch (rc)
	{
	case datalayer::SUCCESS:
		break;
	case datalayer::NOT_FOUND:
	case datalayer::BAD_ENCODING:
	case datalayer::CORRUPTION:
	case datalayer::IO_ERROR:
	case datalayer::LEVELDB_ERROR:
		LOG(ERROR) << "could not make snapshot for search:  " << rc;
		break;
	default:
		abort();
	}
	uint64_t num = 0;
	t_start = po6::monotonic_time();
	while (iter->valid())
	{
		++num;
		iter->next();
	}
	t_end = po6::monotonic_time();
	ostr << " retrieved " << num << " objects in " << t_end - t_start << "ns\n";
	std::string str(ostr.str());
	const char *text = str.c_str();
	size_t text_sz = strlen(text);
	size_t sz = HYPERDEX_HEADER_SIZE_VC
	            + sizeof(uint64_t)
	            + text_sz;
	std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
	msg->pack_at(HYPERDEX_HEADER_SIZE_VC)
	        << nonce << e::pack_memmove(text, text_sz);
	m_daemon->m_comm.send_client(to, from, RESP_SEARCH_DESCRIBE, msg);
}

uint64_t
search_manager :: hash(const id &sid)
{
	return sid.region.get() + sid.client.get() + sid.search_id;
}
