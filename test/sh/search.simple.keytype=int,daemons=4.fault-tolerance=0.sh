#!/bin/sh
SPACE="space search key int number attributes     bit01, bit02, bit03, bit04, bit05, bit06, bit07, bit08,     bit09, bit10, bit11, bit12, bit13, bit14, bit15, bit16,     bit17, bit18, bit19, bit20, bit21, bit22, bit23, bit24,     bit25, bit26, bit27, bit28, bit29, bit30, bit31, bit32 create 4 partitions tolerate 0 failures"
exec python "${HYPERDEX_SRCDIR}"/test/runner.py --daemons=4 --space="${SPACE}" -- \
     "${HYPERDEX_BUILDDIR}"/test/search-stress-test --quiet -h {HOST} -p {PORT} -k int
