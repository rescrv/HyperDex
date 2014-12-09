
#include "common/hypercube.h"


hyperdex::    hypercube:: hypercube(uint16_t * attrs, size_t attrs_sz, uint64_t * lower_coords, uint64_t * upper_coords)
    {
        for (size_t i = 0; i < attrs_sz; ++i)
        {
            attr.push_back(attrs[i]);
            lower_coord.push_back(lower_coords[i]);
            upper_coord.push_back(upper_coords[i]);
        }

    }

hyperdex::    hypercube :: ~hypercube() throw ()
    {
    }

