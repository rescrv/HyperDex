#include "common/datatype_timestamp.h"

// e
#include <e/endian.h>
#include <e/safe_math.h>

#include "cityhash/city.h"

using hyperdex::datatype_info;
using hyperdex::datatype_timestamp;

namespace {



int64_t
unpack(const e::slice& value)
{
    assert(value.size() == sizeof(int64_t) || value.empty());

    if (value.empty()) {
      return 0;
    }

    int64_t timestamp;
    e::unpack64le(value.data(),&timestamp);

    return timestamp;
}
}

datatype_timestamp::datatype_timestamp(enum timestamp_interval interval) {
    this->interval = interval;
}

datatype_timestamp :: ~datatype_timestamp() throw ()
{
}

hyperdatatype
datatype_timestamp::datatype() const
{
  switch(this->interval)
  {
    case SECOND:
      return HYPERDATATYPE_TIMESTAMP_SECOND;
    case MINUTE:
      return HYPERDATATYPE_TIMESTAMP_MINUTE;
    case HOUR:
      return HYPERDATATYPE_TIMESTAMP_HOUR;
    case DAY:
      return HYPERDATATYPE_TIMESTAMP_DAY;
    case WEEK:
      return HYPERDATATYPE_TIMESTAMP_WEEK;
    case MONTH:
      return HYPERDATATYPE_TIMESTAMP_MONTH;

  }
}
bool
datatype_timestamp::validate(const e::slice& value) const {
  return value.size() == sizeof(int64_t) || value.empty();
}

bool
datatype_timestamp::check_args(const funcall& func) const
{
  return func.name == FUNC_SET && func.arg1_datatype == this->datatype() && validate(func.arg1);
}

uint8_t*
datatype_timestamp::apply(const e::slice& old_value,
                       const funcall* funcs, size_t funcs_sz,
                       uint8_t* writeto)
{
  int64_t timestamp;
  for(size_t i = 0; i < funcs_sz; i++ )
  {
    const funcall * func = funcs +i;
    if (func->name != FUNC_SET) {
      abort();
    }

    timestamp = unpack(func->arg1);
  }

  return e::pack64le(timestamp,writeto);
}

bool
datatype_timestamp::hashable() const
{
    return true;
}

const char * INTERVAL_SECOND_STRING = "SECOND";
const char * INTERVAL_MINUTE_STRING = "MINUTE";
const char * INTERVAL_HOUR_STRING = "HOUR";
const char * INTERVAL_DAY_STRING = "DAY";
const char * INTERVAL_WEEK_STRING = "WEEK";
const char * INTERVAL_MONTH_STRING = "MONTH";

const char * interval_to_string(enum timestamp_interval in)
{
    switch(in)
    {
    case SECOND:
        return INTERVAL_SECOND_STRING;
    case MINUTE:
        return INTERVAL_MINUTE_STRING;
    case HOUR:
        return INTERVAL_HOUR_STRING;
    case DAY:
        return INTERVAL_DAY_STRING;
    case WEEK:
        return INTERVAL_WEEK_STRING;
    case MONTH:
        return INTERVAL_MONTH_STRING;
    default:
        abort();
    };
}


int64_t  lookup_interesting[] = { 1, 60, 3600, 24*60*60, 7*24*60*60, 30 *7 * 24*60*60};
uint64_t
datatype_timestamp::hash(const e::slice& value) {

    //TODO better hash
    //std::cout << "Hashing with "<<interval_to_string(this->interval) << "\n";
    int64_t interesting_bits;
    int64_t extra;
    int64_t timestamp = unpack(value);
    interesting_bits = lookup_interesting[this->interval];
    extra = timestamp % interesting_bits;
    timestamp /= interesting_bits;

    return CityHash64((const char*)&timestamp, sizeof(int64_t))+extra;

}

bool
datatype_timestamp :: indexable() const
{
    return true;
}


bool
datatype_timestamp :: containable() const
{
    return true;
}

bool
datatype_timestamp :: step(const uint8_t** ptr,
                       const uint8_t* end,
                       e::slice* elem)
{
    if (static_cast<size_t>(end - *ptr) < sizeof(int64_t))
    {
        return false;
    }

    *elem = e::slice(*ptr, sizeof(int64_t));
    *ptr += sizeof(int64_t);
    return true;
}

uint8_t*
datatype_timestamp :: write(uint8_t* writeto,
                        const e::slice& elem)
{
    memmove(writeto, elem.data(), elem.size());
    return writeto + elem.size();
}

bool
datatype_timestamp :: comparable() const
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
{
    int64_t lhsnum = unpack(lhs);
    int64_t rhsnum = unpack(rhs);

    if (lhsnum < rhsnum)
    {
        return -1;
    }
    if (lhsnum > rhsnum)
    {
        return 1;
    }

    return 0;
}

int
datatype_timestamp :: compare(const e::slice& lhs, const e::slice& rhs)
{
    return ::compare(lhs, rhs);
}

static bool
compare_less(const e::slice& lhs,
             const e::slice& rhs)
{
    return compare(lhs, rhs) < 0;
}

datatype_info::compares_less
datatype_timestamp :: compare_less()
{
    return &::compare_less;
}


