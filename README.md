HyperDex
========

HyperDex is a distributed, searchable, and consistent key-value store.

More information is available at <http://hyperdex.org/>

Dependencies
------------

 * [Google's CityHash 1.0.x](http://code.google.com/p/cityhash/)
 * [Google Test 1.6.x](http://code.google.com/p/googletest/)
 * [Google's glog 0.3.x](http://code.google.com/p/google-glog/)
 * [e 0.2.x](http://hyperdex.org/download/)
 * [po6 0.2.x](http://hyperdex.org/download/)
 * [Python >=2.6.0](http://www.python.org/)
 * [Cython 0.15.x](http://www.cython.org/)
 * [pyparsing 1.5.x](http://pyparsing.wikispaces.com/)

For Java bindings you will need [SWIG](http://www.swig.org/).
To benchmark HyperDex using Yahoo Cloud Serving Benchmark you will also
need [YCSB](https://github.com/brianfrankcooper/YCSB).

For tests coverage statistics you'll need [lcov](http://ltp.sourceforge.net/coverage/lcov.php).

Installation
------------

Manual:

    mkdir build && cd build
    cmake .. && make -j4

Additional cmake options:

 * `-DCMAKE_INSTALL_PREFIX=/usr` - set installation prefix (`/usr/local` by default)
 * `-DENABLE_JAVA_BINDINGS=ON` - enable Java bindings for HyperDex client
 * `-DENABLE_YCSB=ON` - enable Yahoo Cloud Serving Benchmark
 * `-DYCSB_JAR=.../ycsb-0.1.4/core/lib/core-0.1.4.jar` - path to YCSB jar file
 * `-DENABLE_COVERAGE=ON` - build for coverage testing

You may also run `cmake-gui .` after cmake to edit options in a more convinient way.

Testing
-------

    make test

Generate tests coverage statistics:

    make coverage

and open `coverage/index.html` in your browser.
