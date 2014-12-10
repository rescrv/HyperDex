/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef hyperdex_h_
#define hyperdex_h_

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/* This file includes enums and constants that are used throughout HyperDex. */

/* Datatype occupies [9216, 9728)
 * The chosen constants are significant as they allow properties of the datatype
 * to be determined with simple mask operations.
 */

#define CONTAINER_TYPE(X) ((enum hyperdatatype)((X) & 9664))
#define CONTAINER_ELEM(X) ((enum hyperdatatype)((X) & 9223))
#define CONTAINER_VAL(X) ((enum hyperdatatype)((X) & 9223))
#define CONTAINER_KEY(X) ((enum hyperdatatype)((((X) & 56) >> 3) | ((X) & 9216)))
#define IS_PRIMITIVE(X) (CONTAINER_TYPE(X) == HYPERDATATYPE_GENERIC)
#define CREATE_CONTAINER(C, E) ((enum hyperdatatype)((C) | ((E) & 7)))
#define CREATE_CONTAINER2(C, K, V) ((enum hyperdatatype)((C) | (((K) & 7) << 3) | ((V) & 7)))
#define RESTRICT_CONTAINER(C, V) ((enum hyperdatatype)((C) | ((V) & 7)))

enum hyperdatatype
{
    /* Primitive types */
    HYPERDATATYPE_GENERIC   = 9216,
    HYPERDATATYPE_STRING    = 9217,
    HYPERDATATYPE_INT64     = 9218,
    HYPERDATATYPE_FLOAT     = 9219,
    HYPERDATATYPE_DOCUMENT  = 9223,

    /* List types */
    HYPERDATATYPE_LIST_GENERIC = 9280,
    HYPERDATATYPE_LIST_STRING  = 9281,
    HYPERDATATYPE_LIST_INT64   = 9282,
    HYPERDATATYPE_LIST_FLOAT   = 9283,

    /* Set types */
    HYPERDATATYPE_SET_GENERIC = 9344,
    HYPERDATATYPE_SET_STRING  = 9345,
    HYPERDATATYPE_SET_INT64   = 9346,
    HYPERDATATYPE_SET_FLOAT   = 9347,

    /* Map types */
    HYPERDATATYPE_MAP_GENERIC        = 9408,
    HYPERDATATYPE_MAP_STRING_KEYONLY = 9416,
    HYPERDATATYPE_MAP_STRING_STRING  = 9417,
    HYPERDATATYPE_MAP_STRING_INT64   = 9418,
    HYPERDATATYPE_MAP_STRING_FLOAT   = 9419,
    HYPERDATATYPE_MAP_INT64_KEYONLY  = 9424,
    HYPERDATATYPE_MAP_INT64_STRING   = 9425,
    HYPERDATATYPE_MAP_INT64_INT64    = 9426,
    HYPERDATATYPE_MAP_INT64_FLOAT    = 9427,
    HYPERDATATYPE_MAP_FLOAT_KEYONLY  = 9432,
    HYPERDATATYPE_MAP_FLOAT_STRING   = 9433,
    HYPERDATATYPE_MAP_FLOAT_INT64    = 9434,
    HYPERDATATYPE_MAP_FLOAT_FLOAT    = 9435,

    /*Timestamp types */
    HYPERDATATYPE_TIMESTAMP_GENERIC  = 9472,
    HYPERDATATYPE_TIMESTAMP_SECOND   = 9473,
    HYPERDATATYPE_TIMESTAMP_MINUTE   = 9474,
    HYPERDATATYPE_TIMESTAMP_HOUR     = 9475,
    HYPERDATATYPE_TIMESTAMP_DAY      = 9476,
    HYPERDATATYPE_TIMESTAMP_WEEK     = 9477,
    HYPERDATATYPE_TIMESTAMP_MONTH    = 9478,

    /* Special (internal) types */
    HYPERDATATYPE_MACAROON_SECRET    = 9664,

    /* Returned if the server acts up */
    HYPERDATATYPE_GARBAGE   = 9727
};

/* Predicate occupies [9728, 9856) */
enum hyperpredicate
{
    HYPERPREDICATE_FAIL          = 9728,
    HYPERPREDICATE_EQUALS        = 9729,
    HYPERPREDICATE_LESS_THAN     = 9738,
    HYPERPREDICATE_LESS_EQUAL    = 9730,
    HYPERPREDICATE_GREATER_EQUAL = 9731,
    HYPERPREDICATE_GREATER_THAN  = 9739,
    HYPERPREDICATE_CONTAINS_LESS_THAN = 9732, /* alias of HYPERPREDICATE_LENGTH_LESS_EQUAL */
    HYPERPREDICATE_REGEX         = 9733,
    HYPERPREDICATE_LENGTH_EQUALS        = 9734,
    HYPERPREDICATE_LENGTH_LESS_EQUAL    = 9735,
    HYPERPREDICATE_LENGTH_GREATER_EQUAL = 9736,
    HYPERPREDICATE_CONTAINS      = 9737
    /* NEXT = 9740 */
};

#ifdef __cplusplus
} /* extern "C" */

/* C++ */
#include <iostream>

std::ostream&
operator << (std::ostream& lhs, hyperdatatype rhs);
std::ostream&
operator << (std::ostream& lhs, hyperpredicate rhs);

#endif /* __cplusplus */
#endif /* hyperdex_h_ */
