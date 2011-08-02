project(cityhash)
include_directories(include)

# Installed headers.
set (cityhash_headers
)

set (cityhash_srcs
    src/city.cc
)

add_library(cityhash STATIC ${cityhash_srcs})
