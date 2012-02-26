#!/usr/bin/env python

# Copyright (c) 2011, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import collections
import string
import unittest

from pyparsing import Combine, Forward, Group, Literal, Optional, Suppress, ZeroOrMore, Word, delimitedList, stringEnd


Dimension = collections.namedtuple("Dimension", ["name", "replhash", "diskhash"])
Region = collections.namedtuple("Region", ["mask", "prefix", "replicas"])
Subspace = collections.namedtuple("Subspace", ["dimensions", "regions"])
Space = collections.namedtuple("Space", ["name", "dimensions", "subspaces"])


def _encompases(outter, inner):
    '''Check if the outter region contains the inner region.

    This means the outter region is bigger than, or equal to the inner.
    '''
    shared_prefix = min(outter.prefix, inner.prefix)
    shared_mask = ((1 << 64) - 1) & ~((1 << (64 - shared_prefix)) - 1)
    return outter.prefix <= inner.prefix and \
           (outter.mask & shared_mask) == (inner.mask & shared_mask)


def _upper_bound_of(region):
    return region.mask + (1 << (64 - region.prefix))


def _fill_to_region(upper_bound, auto_prefix, auto_replication, target):
    regions = []
    while upper_bound < target:
        boundstr = bin(upper_bound)[2:]
        boundstr = '0' * (65 - len(boundstr)) + boundstr
        targetstr = bin(target)[2:]
        targetstr = '0' * (65 - len(targetstr)) + targetstr
        if targetstr[:auto_prefix + 1] == boundstr[:auto_prefix + 1] and \
           targetstr.startswith(boundstr.rstrip('0')):
            # We need to build ever smaller increments to approach the target.
            index = boundstr.rfind('1') + 1
            index = targetstr.find('1', index)
            assert(index >= 0)
            regions.append(Region(mask=upper_bound,
                                  prefix=index,
                                  replicas=list(auto_replication)))
        elif boundstr[auto_prefix + 1:].strip('0') == '':
            # We need to create an undivided automatic region.
            regions.append(Region(mask=upper_bound,
                                  prefix=auto_prefix,
                                  replicas=list(auto_replication)))
        else:
            # We need finish this divided automatic region.
            index = boundstr.rfind('1')
            assert(index >= 0)
            regions.append(Region(mask=upper_bound,
                                  prefix=index,
                                  replicas=list(auto_replication)))
        upper_bound = _upper_bound_of(regions[-1])
    return regions


def parse_dimension(dim):
    return Dimension(dim[0], dim[1][0], dim[1][1])


def parse_regions(regions):
    # Determine the automatic interval.
    auto_prefix = None
    auto_increment = None
    auto_replication = None
    end_idx = None
    if regions[-1][0] == "auto":
        if regions[-1][1] > 64 or regions[-1][1] < 0:
            raise ValueError("Regions must use use 0 <= prefix <= 64")
        auto_prefix = regions[-1][1]
        auto_increment = 1 << (64 - regions[-1][1])
        auto_replication = [None] * regions[-1][2]
        end_idx = -1
    # Determine the static regions
    staticregions = []
    for region in regions[:end_idx]:
        assert(region[0] in ("region",))
        staticregions.append(Region(mask=region[2], prefix=region[1], replicas=[None] * region[3]))
    staticregions.sort()
    regions = []
    upper_bound = 0
    for region in staticregions:
        if auto_prefix:
            regions += _fill_to_region(upper_bound, auto_prefix,
                                        auto_replication, region.mask)
            if regions:
                upper_bound = _upper_bound_of(regions[-1])
            assert(upper_bound == region.mask)
        if upper_bound != region.mask:
            raise ValueError('Static regions without an "auto" statement must be contiguous')
        regions.append(region)
        upper_bound = _upper_bound_of(regions[-1])
    regions += _fill_to_region(upper_bound, auto_prefix, auto_replication, 1 << 64)
    return regions


def parse_subspace(subspace):
    return Subspace(dimensions=list(subspace[0]), regions=list(subspace[1]))


def parse_space(space):
    dims = [dim.name for dim in list(space.dimensions)]
    if space.key not in dims:
        raise ValueError("Space key must be one of its dimensions.")
    for subspace in space.subspaces:
        for dim in set(subspace.dimensions):
            if dim.name not in dims:
                raise ValueError("Subspace dimension {0} must be one of its dimensions.".format(repr(name)))
    if space.keyhash[0] == "none":
        raise ValueError("Cannot specify \"none\" as a replication hash for dimension {0}".format(repr(space.key)))
    keysubspace = Subspace(dimensions=[Dimension(space.key, *space.keyhash)], regions=list(space.keyregions))
    subspaces = [keysubspace] + list(space.subspaces)
    return Space(name=space.name, dimensions=space.dimensions, subspaces=subspaces)


identifier = Word(string.ascii_letters + string.digits + '_')
integer = Word(string.digits).setParseAction(lambda t: int(t[0]))
hexnum  = Combine(Literal("0x") + Word(string.hexdigits)).setParseAction(lambda t: int(t[0][2:], 16))
hashequal = Literal("equality")
hashrange = Literal("range")
hashnone  = Literal("none")
hashtype  = hashequal|hashrange|hashnone

def hash_specifier(repl_default, disk_default):
    return Optional(Group(Optional(hashtype, default=repl_default) +
                          Suppress(":") +
                          Optional(hashtype, default=disk_default)),
                    default=[repl_default, disk_default])

def dimensions(repl_default, disk_default):
    dimension = identifier.setResultsName("name") + \
                hash_specifier(repl_default, disk_default).setResultsName("hash")
    dimension.setParseAction(parse_dimension)
    return delimitedList(dimension)

autoregion = Literal("auto") + integer + integer
staticregion = Literal("region") + integer + hexnum + integer
region = ZeroOrMore(Group(staticregion)) + Optional(Group(autoregion))
region.setParseAction(parse_regions)
subspace = Literal("subspace").suppress() + Group(dimensions("equality", None)) + Group(region)
subspace.setParseAction(parse_subspace)
space = Literal("space").suppress() + identifier.setResultsName("name") + \
        Literal("dimensions").suppress() + Group(dimensions("none", "equality")).setResultsName("dimensions") + \
        Literal("key").suppress() + identifier.setResultsName("key") + \
        hash_specifier("equality", None).setResultsName("keyhash") + \
        Group(region).setResultsName("keyregions") + \
        ZeroOrMore(subspace).setResultsName("subspaces")
space.setParseAction(parse_space)


class TestFillToRegion(unittest.TestCase):

    def test_already_met(self):
        a = 0
        b = 1
        while a < (1 << 64):
            self.assertEqual([], _fill_to_region(a, 0, "REPL", a))
            a, b = b, a + b

    def test_auto0(self):
        expected = [Region(0, 0, "REPL")]
        returned = _fill_to_region(0, 0, "REPL", 1 << 64)
        self.assertEqual(expected, returned)

    def test_auto1(self):
        expected = [Region(0, 1, "REPL"),
                    Region(0x8000000000000000, 1, "REPL")]
        returned = _fill_to_region(0, 1, "REPL", 1 << 64)
        self.assertEqual(expected, returned)

    def test_auto2(self):
        expected = [Region(0, 2, "REPL"),
                    Region(0x4000000000000000, 2, "REPL"),
                    Region(0x8000000000000000, 2, "REPL"),
                    Region(0xc000000000000000, 2, "REPL")]
        returned = _fill_to_region(0, 2, "REPL", 1 << 64)
        self.assertEqual(expected, returned)

    def test_auto3(self):
        expected = [Region(0, 3, "REPL"),
                    Region(0x2000000000000000, 3, "REPL"),
                    Region(0x4000000000000000, 3, "REPL"),
                    Region(0x6000000000000000, 3, "REPL"),
                    Region(0x8000000000000000, 3, "REPL"),
                    Region(0xa000000000000000, 3, "REPL"),
                    Region(0xc000000000000000, 3, "REPL"),
                    Region(0xe000000000000000, 3, "REPL")]
        returned = _fill_to_region(0, 3, "REPL", 1 << 64)
        self.assertEqual(expected, returned)

    def test_static_with_bigger_upper_bound_prefix(self):
        expected = [Region(0x5500000000000000, 8, "REPL"),
                    Region(0x5600000000000000, 7, "REPL"),
                    Region(0x5800000000000000, 5, "REPL"),
                    Region(0x6000000000000000, 3, "REPL")]
        returned = _fill_to_region(0x5500000000000000, 3, "REPL", 0x8000000000000000)
        self.assertEqual(expected, returned)

    def test_static_with_bigger_target_prefix(self):
        expected = [Region(0x6000000000000000, 3, "REPL"),
                    Region(0x8000000000000000, 3, "REPL"),
                    Region(0xa000000000000000, 5, "REPL"),
                    Region(0xa800000000000000, 7, "REPL")]
        returned = _fill_to_region(0x6000000000000000, 3, "REPL", 0xaa00000000000000)
        self.assertEqual(expected, returned)


class TestRegionParsing(unittest.TestCase):

    def test_solo_autoregion(self):
        expected = ["auto", 3, 3]
        returned = list((autoregion + stringEnd).parseString("auto 3 3"))
        self.assertEqual(expected, returned)

    def test_solo_staticregion(self):
        expected = ["region", 3, 9223372036854775808L, 2]
        returned = list((staticregion + stringEnd).parseString("region 3 0x8000000000000000 2"))
        self.assertEqual(expected, returned)

    def test_autoregion(self):
        expected = [Region(prefix=1, mask=0L, replicas=[None, None, None]),
                    Region(prefix=1, mask=0x8000000000000000, replicas=[None, None, None])]
        returned = list((region + stringEnd).parseString("auto 1 3"))
        self.assertEqual(expected, returned)

    def test_onestatic(self):
        expected = [Region(prefix=1, mask=0L, replicas=[None, None, None]),
                    Region(prefix=1, mask=0x8000000000000000, replicas=[None, None])]
        returned = list((region + stringEnd).parseString("region 1 0x8000000000000000 2 auto 1 3"))
        self.assertEqual(expected, returned)

    def test_static_is_everything(self):
        expected = [Region(prefix=0, mask=0L, replicas=[None, None])]
        returned = list((region + stringEnd).parseString("region 0 0x0000000000000000 2 auto 1 3"))
        self.assertEqual(expected, returned)

    def test_static_has_bigger_prefix_same_mask(self):
        expected = [Region(prefix=1, mask=0L, replicas=[None, None, None]),
                    Region(prefix=2, mask=0x8000000000000000, replicas=[None, None]),
                    Region(prefix=2, mask=0xc000000000000000, replicas=[None, None, None])]
        returned = list((region + stringEnd).parseString("region 2 0x8000000000000000 2 auto 1 3"))
        self.assertEqual(expected, returned)

    def test_static_has_bigger_prefix_different_mask(self):
        expected = [Region(prefix=1, mask=0L, replicas=[None, None, None]),
                    Region(prefix=2, mask=0x8000000000000000, replicas=[None, None, None]),
                    Region(prefix=2, mask=0xc000000000000000, replicas=[None, None])]
        returned = list((region + stringEnd).parseString("region 2 0xc000000000000000 2 auto 1 3"))
        self.assertEqual(expected, returned)

    def test_static_has_smaller_prefix(self):
        expected = [Region(prefix=2, mask=0L, replicas=[None, None, None]),
                    Region(prefix=2, mask=0x4000000000000000, replicas=[None, None, None]),
                    Region(prefix=1, mask=0x8000000000000000, replicas=[None, None])]
        returned = list((region + stringEnd).parseString("region 1 0x8000000000000000 2 auto 2 3"))
        self.assertEqual(expected, returned)

    def test_multiple_static_for_one_auto(self):
        expected = [Region(prefix=3, mask=0L, replicas=[None, None, None]),
                    Region(prefix=4, mask=0x2000000000000000, replicas=[None, None]),
                    Region(prefix=4, mask=0x3000000000000000, replicas=[None, None, None]),
                    Region(prefix=4, mask=0x4000000000000000, replicas=[None, None, None]),
                    Region(prefix=4, mask=0x5000000000000000, replicas=[None, None]),
                    Region(prefix=3, mask=0x6000000000000000, replicas=[None, None, None]),
                    Region(prefix=2, mask=0x8000000000000000, replicas=[None, None, None]),
                    Region(prefix=2, mask=0xc000000000000000, replicas=[None, None, None])]
        returned = list((region + stringEnd).parseString("""region 4 0x2000000000000000 2
                                                            region 4 0x5000000000000000 2
                                                            auto 2 3"""))
        self.assertEqual(expected, returned)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFillToRegion)
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestRegionParsing))
    unittest.TextTestRunner(verbosity=2).run(suite)
