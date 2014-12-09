
#ifndef hyperdex_common_hypercube_h_
#define hyperdex_common_hypercube_h_

// STL
#include <vector>

#include <inttypes.h>
//for size_t
#include <cstring>
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE
class hypercube;

class hypercube
{
    public:
        hypercube(uint16_t * attrs, size_t attrs_sz, uint64_t * lower_coord, uint64_t * upper_coord);
        ~hypercube() throw();

    public:
        std::vector<uint16_t> attr;
        std::vector<uint64_t> lower_coord;
        std::vector<uint64_t> upper_coord;
};
END_HYPERDEX_NAMESPACE
#endif
