package hypergo

// User-facing APIs

/*
#cgo LDFLAGS: -lhyperdex-client
#include <netinet/in.h>
#include "hyperdex/client.h"
*/
import "C"

func (client *Client) Get(space, key string) Object {
	return <-client.AsyncGet(space, key)
}

func (client *Client) AsyncGet(space, key string) ObjectChannel {
	return client.objOp("get", space, key, nil)
}

func (client *Client) Put(space, key string, attrs Attributes) error {
	return <-client.AsyncPut(space, key, attrs)
}

func (client *Client) AsyncPut(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("put", space, key, attrs, nil)
}

func (client *Client) CondPut(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondPut(space, key, attrs, conds)
}

func (client *Client) AsyncCondPut(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_put", space, key, attrs, conds)
}

func (client *Client) PutIfNotExist(space, key string, attrs Attributes) error {
	return <-client.AsyncPutIfNotExist(space, key, attrs)
}

func (client *Client) AsyncPutIfNotExist(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("put_if_not_exist", space, key, attrs, nil)
}

func (client *Client) Delete(space, key string) error {
	return <-client.AsyncDelete(space, key)
}

func (client *Client) AsyncDelete(space, key string) ErrorChannel {
	return client.errOp("del", space, key, nil, nil)
}

func (client *Client) CondDelete(space, key string, conds []Condition) error {
	return <-client.AsyncCondDelete(space, key, conds)
}

func (client *Client) AsyncCondDelete(space, key string, conds []Condition) ErrorChannel {
	return client.errOp("cond_del", space, key, nil, conds)
}

func (client *Client) AtomicAdd(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicAdd(space, key, attrs)
}

func (client *Client) AsyncAtomicAdd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_add", space, key, attrs, nil)
}

func (client *Client) CondAtomicAdd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicAdd(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicAdd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_add", space, key, attrs, conds)
}

func (client *Client) AtomicSub(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicSub(space, key, attrs)
}

func (client *Client) AsyncAtomicSub(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_sub", space, key, attrs, nil)
}

func (client *Client) CondAtomicSub(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicSub(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicSub(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_sub", space, key, attrs, conds)
}

func (client *Client) AtomicMul(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicMul(space, key, attrs)
}

func (client *Client) AsyncAtomicMul(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_mul", space, key, attrs, nil)
}

func (client *Client) CondAtomicMul(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicMul(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicMul(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_mul", space, key, attrs, conds)
}

func (client *Client) AtomicDiv(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicDiv(space, key, attrs)
}

func (client *Client) AsyncAtomicDiv(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_div", space, key, attrs, nil)
}

func (client *Client) CondAtomicDiv(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicDiv(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicDiv(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_div", space, key, attrs, conds)
}

func (client *Client) AtomicMod(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicMod(space, key, attrs)
}

func (client *Client) AsyncAtomicMod(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_mod", space, key, attrs, nil)
}

func (client *Client) CondAtomicMod(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicMod(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicMod(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_mod", space, key, attrs, conds)
}

func (client *Client) AtomicAnd(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicAnd(space, key, attrs)
}

func (client *Client) AsyncAtomicAnd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_and", space, key, attrs, nil)
}

func (client *Client) CondAtomicAnd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicAnd(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicAnd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_and", space, key, attrs, conds)
}

func (client *Client) AtomicOr(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicOr(space, key, attrs)
}

func (client *Client) AsyncAtomicOr(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_or", space, key, attrs, nil)
}

func (client *Client) CondAtomicOr(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicOr(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicOr(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_or", space, key, attrs, conds)
}

func (client *Client) AtomicXor(space, key string, attrs Attributes) error {
	return <-client.AsyncAtomicXor(space, key, attrs)
}

func (client *Client) AsyncAtomicXor(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("atomic_xor", space, key, attrs, nil)
}

func (client *Client) CondAtomicXor(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondAtomicXor(space, key, attrs, conds)
}

func (client *Client) AsyncCondAtomicXor(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_atomic_xor", space, key, attrs, conds)
}

func (client *Client) StringPrepend(space, key string, attrs Attributes) error {
	return <-client.AsyncStringPrepend(space, key, attrs)
}

func (client *Client) AsyncStringPrepend(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("string_prepend", space, key, attrs, nil)
}

func (client *Client) CondStringPrepend(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondStringPrepend(space, key, attrs, conds)
}

func (client *Client) AsyncCondStringPrepend(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_string_prepend", space, key, attrs, conds)
}

func (client *Client) StringAppend(space, key string, attrs Attributes) error {
	return <-client.AsyncStringAppend(space, key, attrs)
}

func (client *Client) AsyncStringAppend(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("string_append", space, key, attrs, nil)
}

func (client *Client) CondStringAppend(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondStringAppend(space, key, attrs, conds)
}

func (client *Client) AsyncCondStringAppend(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_string_append", space, key, attrs, conds)
}

func (client *Client) ListLPush(space, key string, attrs Attributes) error {
	return <-client.AsyncListLPush(space, key, attrs)
}

func (client *Client) AsyncListLPush(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("list_lpush", space, key, attrs, nil)
}

func (client *Client) CondListLPush(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondListLPush(space, key, attrs, conds)
}

func (client *Client) AsyncCondListLPush(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_list_lpush", space, key, attrs, conds)
}

func (client *Client) ListRPush(space, key string, attrs Attributes) error {
	return <-client.AsyncListRPush(space, key, attrs)
}

func (client *Client) AsyncListRPush(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("list_rpush", space, key, attrs, nil)
}

func (client *Client) CondListRPush(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondListRPush(space, key, attrs, conds)
}

func (client *Client) AsyncCondListRPush(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_list_rpush", space, key, attrs, conds)
}

func (client *Client) SetAdd(space, key string, attrs Attributes) error {
	return <-client.AsyncSetAdd(space, key, attrs)
}

func (client *Client) AsyncSetAdd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("set_add", space, key, attrs, nil)
}

func (client *Client) CondSetAdd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondSetAdd(space, key, attrs, conds)
}

func (client *Client) AsyncCondSetAdd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_set_add", space, key, attrs, conds)
}

func (client *Client) SetRemove(space, key string, attrs Attributes) error {
	return <-client.AsyncSetRemove(space, key, attrs)
}

func (client *Client) AsyncSetRemove(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("set_remove", space, key, attrs, nil)
}

func (client *Client) CondSetRemove(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondSetRemove(space, key, attrs, conds)
}

func (client *Client) AsyncCondSetRemove(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_set_remove", space, key, attrs, conds)
}

func (client *Client) SetIntersect(space, key string, attrs Attributes) error {
	return <-client.AsyncSetIntersect(space, key, attrs)
}

func (client *Client) AsyncSetIntersect(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("set_intersect", space, key, attrs, nil)
}

func (client *Client) CondSetIntersect(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondSetIntersect(space, key, attrs, conds)
}

func (client *Client) AsyncCondSetIntersect(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_set_intersect", space, key, attrs, conds)
}

func (client *Client) SetUnion(space, key string, attrs Attributes) error {
	return <-client.AsyncSetUnion(space, key, attrs)
}

func (client *Client) AsyncSetUnion(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("set_union", space, key, attrs, nil)
}

func (client *Client) CondSetUnion(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondSetUnion(space, key, attrs, conds)
}

func (client *Client) AsyncCondSetUnion(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_set_union", space, key, attrs, conds)
}

func (client *Client) MapAdd(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAdd(space, key, attrs)
}

func (client *Client) AsyncMapAdd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_add", space, key, attrs, nil)
}

func (client *Client) CondMapAdd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAdd(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAdd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_add", space, key, attrs, conds)
}

func (client *Client) MapRemove(space, key string, attrs Attributes) error {
	return <-client.AsyncMapRemove(space, key, attrs)
}

func (client *Client) AsyncMapRemove(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_remove", space, key, attrs, nil)
}

func (client *Client) CondMapRemove(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapRemove(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapRemove(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_remove", space, key, attrs, conds)
}

func (client *Client) MapAtomicAdd(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicAdd(space, key, attrs)
}

func (client *Client) AsyncMapAtomicAdd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_add", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicAdd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicAdd(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicAdd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_add", space, key, attrs, conds)
}

func (client *Client) MapAtomicSub(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicSub(space, key, attrs)
}

func (client *Client) AsyncMapAtomicSub(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_sub", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicSub(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicSub(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicSub(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_sub", space, key, attrs, conds)
}

func (client *Client) MapAtomicMul(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicMul(space, key, attrs)
}

func (client *Client) AsyncMapAtomicMul(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_mul", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicMul(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicMul(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicMul(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_mul", space, key, attrs, conds)
}

func (client *Client) MapAtomicDiv(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicDiv(space, key, attrs)
}

func (client *Client) AsyncMapAtomicDiv(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_div", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicDiv(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicDiv(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicDiv(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_div", space, key, attrs, conds)
}

func (client *Client) MapAtomicMod(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicMod(space, key, attrs)
}

func (client *Client) AsyncMapAtomicMod(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_mod", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicMod(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicMod(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicMod(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_mod", space, key, attrs, conds)
}

func (client *Client) MapAtomicAnd(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicMul(space, key, attrs)
}

func (client *Client) AsyncMapAtomicAnd(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_and", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicAnd(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicMul(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicAnd(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_and", space, key, attrs, conds)
}

func (client *Client) MapAtomicOr(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicOr(space, key, attrs)
}

func (client *Client) AsyncMapAtomicOr(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_or", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicOr(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicOr(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicOr(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_or", space, key, attrs, conds)
}

func (client *Client) MapAtomicXor(space, key string, attrs Attributes) error {
	return <-client.AsyncMapAtomicXor(space, key, attrs)
}

func (client *Client) AsyncMapAtomicXor(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_atomic_xor", space, key, attrs, nil)
}

func (client *Client) CondMapAtomicXor(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapAtomicXor(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapAtomicXor(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_atomic_xor", space, key, attrs, conds)
}

func (client *Client) MapStringPrepend(space, key string, attrs Attributes) error {
	return <-client.AsyncMapStringPrepend(space, key, attrs)
}

func (client *Client) AsyncMapStringPrepend(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_string_prepend", space, key, attrs, nil)
}

func (client *Client) CondMapStringPrepend(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapStringPrepend(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapStringPrepend(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_string_prepend", space, key, attrs, conds)
}

func (client *Client) MapStringAppend(space, key string, attrs Attributes) error {
	return <-client.AsyncMapStringAppend(space, key, attrs)
}

func (client *Client) AsyncMapStringAppend(space, key string, attrs Attributes) ErrorChannel {
	return client.errOp("map_string_append", space, key, attrs, nil)
}

func (client *Client) CondMapStringAppend(space, key string, attrs Attributes, conds []Condition) error {
	return <-client.AsyncCondMapStringAppend(space, key, attrs, conds)
}

func (client *Client) AsyncCondMapStringAppend(space, key string, attrs Attributes, conds []Condition) ErrorChannel {
	return client.errOp("cond_map_string_append", space, key, attrs, conds)
}

func (client *Client) Search(space string, conds []Condition) ObjectChannel {
	return client.objOp("search", space, "", conds)
}

func (client *Client) SortedSearch(space string, conds []Condition, sort_by string,
	limit int, maxmin int) ObjectChannel {
	return nil
}

func (client *Client) GroupDel(space string, conds []Condition) ErrorChannel {
	return client.AsyncGroupDel(space, conds)
}

func (client *Client) AsyncGroupDel(space string, conds []Condition) ErrorChannel {
	return client.errOp("group_del", space, "", nil, conds)
}

func (client *Client) Count(space string, conds []Condition) int {
	return 0
}
