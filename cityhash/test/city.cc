// Copyright (c) 2011 Google, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include <cstdio>
#include <iostream>
#include <string.h>
#include "cityhash/city.h"

using std::cout;
using std::cerr;
using std::hex;

typedef uint8_t uint8;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef std::pair<uint64, uint64> uint128;

static const uint64 k0 = 0xc3a5c85c97cb3127ULL;
static const uint64 kSeed0 = 1234567;
static const uint64 kSeed1 = k0;
static const uint128 kSeed128(kSeed0, kSeed1);
static const int kDataSize = 1 << 20;
static const int kTestSize = 300;

static char data[kDataSize];

static int errors = 0;  // global error count

// Initialize data to pseudorandom values.
void setup()
{
	uint64 a = 9;
	uint64 b = 777;
	for (int i = 0; i < kDataSize; i++)
	{
		a += b;
		b += a;
		a = (a ^ (a >> 41)) * k0;
		b = (b ^ (b >> 41)) * k0 + i;
		uint8 u = b >> 37;
		memcpy(data + i, &u, 1);  // uint8 -> char
	}
}

#if 1

#define C(x) 0x ## x ## ULL
static const uint64 testdata[kTestSize][16] =
{
	{
		C(9ae16a3b2f90404f), C(75106db890237a4a), C(3feac5f636039766), C(3df09dfc64c09a2b), C(3cb540c392e51e29), C(6b56343feac0663), C(5b7bc50fd8e8ad92),
		C(3df09dfc64c09a2b), C(3cb540c392e51e29), C(6b56343feac0663), C(5b7bc50fd8e8ad92),
		C(95162f24e6a5f930), C(6808bdf4f1eb06e0), C(b3b1f3a67b624d82), C(c9a62f12bd4cd80b),
		C(dc56d17a)
	},
	{
		C(541150e87f415e96), C(1aef0d24b3148a1a), C(bacc300e1e82345a), C(c3cdc41e1df33513), C(2c138ff2596d42f6), C(f58e9082aed3055f), C(162e192b2957163d),
		C(c3cdc41e1df33513), C(2c138ff2596d42f6), C(f58e9082aed3055f), C(162e192b2957163d),
		C(fb99e85e0d16f90c), C(608462c15bdf27e8), C(e7d2c5c943572b62), C(1baaa9327642798c),
		C(99929334)
	},
	{
		C(f3786a4b25827c1), C(34ee1a2bf767bd1c), C(2f15ca2ebfb631f2), C(3149ba1dac77270d), C(70e2e076e30703c), C(59bcc9659bc5296), C(9ecbc8132ae2f1d7),
		C(3149ba1dac77270d), C(70e2e076e30703c), C(59bcc9659bc5296), C(9ecbc8132ae2f1d7),
		C(a01d30789bad7cf2), C(ae03fe371981a0e0), C(127e3883b8788934), C(d0ac3d4c0a6fca32),
		C(4252edb7)
	},
	{
		C(ef923a7a1af78eab), C(79163b1e1e9a9b18), C(df3b2aca6e1e4a30), C(2193fb7620cbf23b), C(8b6a8ff06cda8302), C(1a44469afd3e091f), C(8b0449376612506),
		C(2193fb7620cbf23b), C(8b6a8ff06cda8302), C(1a44469afd3e091f), C(8b0449376612506),
		C(e9d9d41c32ad91d1), C(b44ab09f58e3c608), C(19e9175f9fcf784), C(839b3c9581b4a480),
		C(ebc34f3c)
	},
	{
		C(11df592596f41d88), C(843ec0bce9042f9c), C(cce2ea1e08b1eb30), C(4d09e42f09cc3495), C(666236631b9f253b), C(d28b3763cd02b6a3), C(43b249e57c4d0c1b),
		C(4d09e42f09cc3495), C(666236631b9f253b), C(d28b3763cd02b6a3), C(43b249e57c4d0c1b),
		C(3887101c8adea101), C(8a9355d4efc91df0), C(3e610944cc9fecfd), C(5bf9eb60b08ac0ce),
		C(26f2b463)
	},
	{
		C(831f448bdc5600b3), C(62a24be3120a6919), C(1b44098a41e010da), C(dc07df53b949c6b), C(d2b11b2081aeb002), C(d212b02c1b13f772), C(c0bed297b4be1912),
		C(dc07df53b949c6b), C(d2b11b2081aeb002), C(d212b02c1b13f772), C(c0bed297b4be1912),
		C(682d3d2ad304e4af), C(40e9112a655437a1), C(268b09f7ee09843f), C(6b9698d43859ca47),
		C(b042c047)
	},
	{
		C(3eca803e70304894), C(d80de767e4a920a), C(a51cfbb292efd53d), C(d183dcda5f73edfa), C(3a93cbf40f30128c), C(1a92544d0b41dbda), C(aec2c4bee81975e1),
		C(d183dcda5f73edfa), C(3a93cbf40f30128c), C(1a92544d0b41dbda), C(aec2c4bee81975e1),
		C(5f91814d1126ba4b), C(f8ac57eee87fcf1f), C(c55c644a5d0023cd), C(adb761e827825ff2),
		C(e73bb0a8)
	},
	{
		C(1b5a063fb4c7f9f1), C(318dbc24af66dee9), C(10ef7b32d5c719af), C(b140a02ef5c97712), C(b7d00ef065b51b33), C(635121d532897d98), C(532daf21b312a6d6),
		C(b140a02ef5c97712), C(b7d00ef065b51b33), C(635121d532897d98), C(532daf21b312a6d6),
		C(c0b09b75d943910), C(8c84dfb5ef2a8e96), C(e5c06034b0353433), C(3170faf1c33a45dd),
		C(91dfdd75)
	},
	{
		C(a0f10149a0e538d6), C(69d008c20f87419f), C(41b36376185b3e9e), C(26b6689960ccf81d), C(55f23b27bb9efd94), C(3a17f6166dd765db), C(c891a8a62931e782),
		C(26b6689960ccf81d), C(55f23b27bb9efd94), C(3a17f6166dd765db), C(c891a8a62931e782),
		C(23852dc37ddd2607), C(8b7f1b1ec897829e), C(d1d69452a54eed8a), C(56431f2bd766ec24),
		C(c87f95de)
	},
	{
		C(fb8d9c70660b910b), C(a45b0cc3476bff1b), C(b28d1996144f0207), C(98ec31113e5e35d2), C(5e4aeb853f1b9aa7), C(bcf5c8fe4465b7c8), C(b1ea3a8243996f15),
		C(98ec31113e5e35d2), C(5e4aeb853f1b9aa7), C(bcf5c8fe4465b7c8), C(b1ea3a8243996f15),
		C(cabbccedb6407571), C(d1e40a84c445ec3a), C(33302aa908cf4039), C(9f15f79211b5cdf8),
		C(3f5538ef)
	},
	{
		C(236827beae282a46), C(e43970221139c946), C(4f3ac6faa837a3aa), C(71fec0f972248915), C(2170ec2061f24574), C(9eb346b6caa36e82), C(2908f0fdbca48e73),
		C(71fec0f972248915), C(2170ec2061f24574), C(9eb346b6caa36e82), C(2908f0fdbca48e73),
		C(8101c99f07c64abb), C(b9f4b02b1b6a96a7), C(583a2b10cd222f88), C(199dae4cf9db24c),
		C(70eb1a1f)
	},
	{
		C(c385e435136ecf7c), C(d9d17368ff6c4a08), C(1b31eed4e5251a67), C(df01a322c43a6200), C(298b65a1714b5a7e), C(933b83f0aedf23c), C(157bcb44d63f765a),
		C(df01a322c43a6200), C(298b65a1714b5a7e), C(933b83f0aedf23c), C(157bcb44d63f765a),
		C(d6e9fc7a272d8b51), C(3ee5073ef1a9b777), C(63149e31fac02c59), C(2f7979ff636ba1d8),
		C(cfd63b83)
	},
	{
		C(e3f6828b6017086d), C(21b4d1900554b3b0), C(bef38be1809e24f1), C(d93251758985ee6c), C(32a9e9f82ba2a932), C(3822aacaa95f3329), C(db349b2f90a490d8),
		C(d93251758985ee6c), C(32a9e9f82ba2a932), C(3822aacaa95f3329), C(db349b2f90a490d8),
		C(8d49194a894a19ca), C(79a78b06e42738e6), C(7e0f1eda3d390c66), C(1c291d7e641100a5),
		C(894a52ef)
	},
	{
		C(851fff285561dca0), C(4d1277d73cdf416f), C(28ccffa61010ebe2), C(77a4ccacd131d9ee), C(e1d08eeb2f0e29aa), C(70b9e3051383fa45), C(582d0120425caba),
		C(77a4ccacd131d9ee), C(e1d08eeb2f0e29aa), C(70b9e3051383fa45), C(582d0120425caba),
		C(a740eef1846e4564), C(572dddb74ac3ae00), C(fdb5ca9579163bbd), C(a649b9b799c615d2),
		C(9cde6a54)
	},
	{
		C(61152a63595a96d9), C(d1a3a91ef3a7ba45), C(443b6bb4a493ad0c), C(a154296d11362d06), C(d0f0bf1f1cb02fc1), C(ccb87e09309f90d1), C(b24a8e4881911101),
		C(a154296d11362d06), C(d0f0bf1f1cb02fc1), C(ccb87e09309f90d1), C(b24a8e4881911101),
		C(1a481b4528559f58), C(bf837a3150896995), C(4989ef6b941a3757), C(2e725ab72d0b2948),
		C(6c4898d5)
	},
	{
		C(44473e03be306c88), C(30097761f872472a), C(9fd1b669bfad82d7), C(3bab18b164396783), C(47e385ff9d4c06f), C(18062081bf558df), C(63416eb68f104a36),
		C(3bab18b164396783), C(47e385ff9d4c06f), C(18062081bf558df), C(63416eb68f104a36),
		C(4abda1560c47ac80), C(1ea0e63dc6587aee), C(33ec79d92ebc1de), C(94f9dccef771e048),
		C(13e1978e)
	},
	{
		C(3ead5f21d344056), C(fb6420393cfb05c3), C(407932394cbbd303), C(ac059617f5906673), C(94d50d3dcd3069a7), C(2b26c3b92dea0f0), C(99b7374cc78fc3fb),
		C(ac059617f5906673), C(94d50d3dcd3069a7), C(2b26c3b92dea0f0), C(99b7374cc78fc3fb),
		C(1a8e3c73cdd40ee8), C(cbb5fca06747f45b), C(ceec44238b291841), C(28bf35cce9c90a25),
		C(51b4ba8)
	},
	{
		C(6abbfde37ee03b5b), C(83febf188d2cc113), C(cda7b62d94d5b8ee), C(a4375590b8ae7c82), C(168fd42f9ecae4ff), C(23bbde43de2cb214), C(a8c333112a243c8c),
		C(a4375590b8ae7c82), C(168fd42f9ecae4ff), C(23bbde43de2cb214), C(a8c333112a243c8c),
		C(10ac012e8c518b49), C(64a44605d8b29458), C(a67e701d2a679075), C(3a3a20f43ec92303),
		C(b6b06e40)
	},
	{
		C(943e7ed63b3c080), C(1ef207e9444ef7f8), C(ef4a9f9f8c6f9b4a), C(6b54fc38d6a84108), C(32f4212a47a4665), C(6b5a9a8f64ee1da6), C(9f74e86c6da69421),
		C(6b54fc38d6a84108), C(32f4212a47a4665), C(6b5a9a8f64ee1da6), C(9f74e86c6da69421),
		C(946dd0cb30c1a08e), C(fdf376956907eaaa), C(a59074c6eec03028), C(b1a3abcf283f34ac),
		C(240a2f2)
	},
	{
		C(d72ce05171ef8a1a), C(c6bd6bd869203894), C(c760e6396455d23a), C(f86af0b40dcce7b), C(8d3c15d613394d3c), C(491e400491cd4ece), C(7c19d3530ea3547f),
		C(f86af0b40dcce7b), C(8d3c15d613394d3c), C(491e400491cd4ece), C(7c19d3530ea3547f),
		C(1362963a1dc32af9), C(fb9bc11762e1385c), C(9e164ef1f5376083), C(6c15819b5e828a7e),
		C(5dcefc30)
	},
	{
		C(4182832b52d63735), C(337097e123eea414), C(b5a72ca0456df910), C(7ebc034235bc122f), C(d9a7783d4edd8049), C(5f8b04a15ae42361), C(fc193363336453dd),
		C(7ebc034235bc122f), C(d9a7783d4edd8049), C(5f8b04a15ae42361), C(fc193363336453dd),
		C(9b6c50224ef8c4f8), C(ba225c7942d16c3f), C(6f6d55226a73c412), C(abca061fe072152a),
		C(7a48b105)
	},
	{
		C(d6cdae892584a2cb), C(58de0fa4eca17dcd), C(43df30b8f5f1cb00), C(9e4ea5a4941e097d), C(547e048d5a9daaba), C(eb6ecbb0b831d185), C(e0168df5fad0c670),
		C(9e4ea5a4941e097d), C(547e048d5a9daaba), C(eb6ecbb0b831d185), C(e0168df5fad0c670),
		C(afa9705f98c2c96a), C(749436f48137a96b), C(759c041fc21df486), C(b23bf400107aa2ec),
		C(fd55007b)
	},
	{
		C(5c8e90bc267c5ee4), C(e9ae044075d992d9), C(f234cbfd1f0a1e59), C(ce2744521944f14c), C(104f8032f99dc152), C(4e7f425bfac67ca7), C(9461b911a1c6d589),
		C(ce2744521944f14c), C(104f8032f99dc152), C(4e7f425bfac67ca7), C(9461b911a1c6d589),
		C(5e5ecc726db8b60d), C(cce68b0586083b51), C(8a7f8e54a9cba0fc), C(42f010181d16f049),
		C(6b95894c)
	},
	{
		C(bbd7f30ac310a6f3), C(b23b570d2666685f), C(fb13fb08c9814fe7), C(4ee107042e512374), C(1e2c8c0d16097e13), C(210c7500995aa0e6), C(6c13190557106457),
		C(4ee107042e512374), C(1e2c8c0d16097e13), C(210c7500995aa0e6), C(6c13190557106457),
		C(a99b31c96777f381), C(8312ae8301d386c0), C(ed5042b2a4fa96a3), C(d71d1bb23907fe97),
		C(3360e827)
	},
	{
		C(36a097aa49519d97), C(8204380a73c4065), C(77c2004bdd9e276a), C(6ee1f817ce0b7aee), C(e9dcb3507f0596ca), C(6bc63c666b5100e2), C(e0b056f1821752af),
		C(6ee1f817ce0b7aee), C(e9dcb3507f0596ca), C(6bc63c666b5100e2), C(e0b056f1821752af),
		C(8ea1114e60292678), C(904b80b46becc77), C(46cd9bb6e9dff52f), C(4c91e3b698355540),
		C(45177e0b)
	},
	{
		C(dc78cb032c49217), C(112464083f83e03a), C(96ae53e28170c0f5), C(d367ff54952a958), C(cdad930657371147), C(aa24dc2a9573d5fe), C(eb136daa89da5110),
		C(d367ff54952a958), C(cdad930657371147), C(aa24dc2a9573d5fe), C(eb136daa89da5110),
		C(de623005f6d46057), C(b50c0c92b95e9b7f), C(a8aa54050b81c978), C(573fb5c7895af9b5),
		C(7c6fffe4)
	},
	{
		C(441593e0da922dfe), C(936ef46061469b32), C(204a1921197ddd87), C(50d8a70e7a8d8f56), C(256d150ae75dab76), C(e81f4c4a1989036a), C(d0f8db365f9d7e00),
		C(50d8a70e7a8d8f56), C(256d150ae75dab76), C(e81f4c4a1989036a), C(d0f8db365f9d7e00),
		C(753d686677b14522), C(9f76e0cb6f2d0a66), C(ab14f95988ec0d39), C(97621d9da9c9812f),
		C(bbc78da4)
	},
	{
		C(2ba3883d71cc2133), C(72f2bbb32bed1a3c), C(27e1bd96d4843251), C(a90f761e8db1543a), C(c339e23c09703cd8), C(f0c6624c4b098fd3), C(1bae2053e41fa4d9),
		C(a90f761e8db1543a), C(c339e23c09703cd8), C(f0c6624c4b098fd3), C(1bae2053e41fa4d9),
		C(3589e273c22ba059), C(63798246e5911a0b), C(18e710ec268fc5dc), C(714a122de1d074f3),
		C(c5c25d39)
	},
	{
		C(f2b6d2adf8423600), C(7514e2f016a48722), C(43045743a50396ba), C(23dacb811652ad4f), C(c982da480e0d4c7d), C(3a9c8ed5a399d0a9), C(951b8d084691d4e4),
		C(23dacb811652ad4f), C(c982da480e0d4c7d), C(3a9c8ed5a399d0a9), C(951b8d084691d4e4),
		C(d9f87b4988cff2f7), C(217a191d986aa3bc), C(6ad23c56b480350), C(dd78673938ceb2e7),
		C(b6e5d06e)
	},
	{
		C(38fffe7f3680d63c), C(d513325255a7a6d1), C(31ed47790f6ca62f), C(c801faaa0a2e331f), C(491dbc58279c7f88), C(9c0178848321c97a), C(9d934f814f4d6a3c),
		C(c801faaa0a2e331f), C(491dbc58279c7f88), C(9c0178848321c97a), C(9d934f814f4d6a3c),
		C(606a3e4fc8763192), C(bc15cb36a677ee84), C(52d5904157e1fe71), C(1588dd8b1145b79b),
		C(6178504e)
	},
	{
		C(b7477bf0b9ce37c6), C(63b1c580a7fd02a4), C(f6433b9f10a5dac), C(68dd76db9d64eca7), C(36297682b64b67), C(42b192d71f414b7a), C(79692cef44fa0206),
		C(68dd76db9d64eca7), C(36297682b64b67), C(42b192d71f414b7a), C(79692cef44fa0206),
		C(f0979252f4776d07), C(4b87cd4f1c9bbf52), C(51b84bbc6312c710), C(150720fbf85428a7),
		C(bd4c3637)
	},
	{
		C(55bdb0e71e3edebd), C(c7ab562bcf0568bc), C(43166332f9ee684f), C(b2e25964cd409117), C(a010599d6287c412), C(fa5d6461e768dda2), C(cb3ce74e8ec4f906),
		C(b2e25964cd409117), C(a010599d6287c412), C(fa5d6461e768dda2), C(cb3ce74e8ec4f906),
		C(6120abfd541a2610), C(aa88b148cc95794d), C(2686ca35df6590e3), C(c6b02d18616ce94d),
		C(6e7ac474)
	},
	{
		C(782fa1b08b475e7), C(fb7138951c61b23b), C(9829105e234fb11e), C(9a8c431f500ef06e), C(d848581a580b6c12), C(fecfe11e13a2bdb4), C(6c4fa0273d7db08c),
		C(9a8c431f500ef06e), C(d848581a580b6c12), C(fecfe11e13a2bdb4), C(6c4fa0273d7db08c),
		C(482f43bf5ae59fcb), C(f651fbca105d79e6), C(f09f78695d865817), C(7a99d0092085cf47),
		C(1fb4b518)
	},
	{
		C(c5dc19b876d37a80), C(15ffcff666cfd710), C(e8c30c72003103e2), C(7870765b470b2c5d), C(78a9103ff960d82), C(7bb50ffc9fac74b3), C(477e70ab2b347db2),
		C(7870765b470b2c5d), C(78a9103ff960d82), C(7bb50ffc9fac74b3), C(477e70ab2b347db2),
		C(a625238bdf7c07cf), C(1128d515174809f5), C(b0f1647e82f45873), C(17792d1c4f222c39),
		C(31d13d6d)
	},
	{
		C(5e1141711d2d6706), C(b537f6dee8de6933), C(3af0a1fbbe027c54), C(ea349dbc16c2e441), C(38a7455b6a877547), C(5f97b9750e365411), C(e8cde7f93af49a3),
		C(ea349dbc16c2e441), C(38a7455b6a877547), C(5f97b9750e365411), C(e8cde7f93af49a3),
		C(ba101925ec1f7e26), C(d5e84cab8192c71e), C(e256427726fdd633), C(a4f38e2c6116890d),
		C(26fa72e3)
	},
	{
		C(782edf6da001234f), C(f48cbd5c66c48f3), C(808754d1e64e2a32), C(5d9dde77353b1a6d), C(11f58c54581fa8b1), C(da90fa7c28c37478), C(5e9a2eafc670a88a),
		C(5d9dde77353b1a6d), C(11f58c54581fa8b1), C(da90fa7c28c37478), C(5e9a2eafc670a88a),
		C(e35e1bc172e011ef), C(bf9255a4450ae7fe), C(55f85194e26bc55f), C(4f327873e14d0e54),
		C(6a7433bf)
	},
	{
		C(d26285842ff04d44), C(8f38d71341eacca9), C(5ca436f4db7a883c), C(bf41e5376b9f0eec), C(2252d21eb7e1c0e9), C(f4b70a971855e732), C(40c7695aa3662afd),
		C(bf41e5376b9f0eec), C(2252d21eb7e1c0e9), C(f4b70a971855e732), C(40c7695aa3662afd),
		C(770fe19e16ab73bb), C(d603ebda6393d749), C(e58c62439aa50dbd), C(96d51e5a02d2d7cf),
		C(4e6df758)
	},
	{
		C(c6ab830865a6bae6), C(6aa8e8dd4b98815c), C(efe3846713c371e5), C(a1924cbf0b5f9222), C(7f4872369c2b4258), C(cd6da30530f3ea89), C(b7f8b9a704e6cea1),
		C(a1924cbf0b5f9222), C(7f4872369c2b4258), C(cd6da30530f3ea89), C(b7f8b9a704e6cea1),
		C(fa06ff40433fd535), C(fb1c36fe8f0737f1), C(bb7050561171f80), C(b1bc23235935d897),
		C(d57f63ea)
	},
	{
		C(44b3a1929232892), C(61dca0e914fc217), C(a607cc142096b964), C(f7dbc8433c89b274), C(2f5f70581c9b7d32), C(39bf5e5fec82dcca), C(8ade56388901a619),
		C(f7dbc8433c89b274), C(2f5f70581c9b7d32), C(39bf5e5fec82dcca), C(8ade56388901a619),
		C(c1c6a725caab3ea9), C(c1c7906c2f80b898), C(9c3871a04cc884e6), C(df01813cbbdf217f),
		C(52ef73b3)
	},
	{
		C(4b603d7932a8de4f), C(fae64c464b8a8f45), C(8fafab75661d602a), C(8ffe870ef4adc087), C(65bea2be41f55b54), C(82f3503f636aef1), C(5f78a282378b6bb0),
		C(8ffe870ef4adc087), C(65bea2be41f55b54), C(82f3503f636aef1), C(5f78a282378b6bb0),
		C(7bf2422c0beceddb), C(9d238d4780114bd), C(7ad198311906597f), C(ec8f892c0422aca3),
		C(3cb36c3)
	},
	{
		C(4ec0b54cf1566aff), C(30d2c7269b206bf4), C(77c22e82295e1061), C(3df9b04434771542), C(feddce785ccb661f), C(a644aff716928297), C(dd46aee73824b4ed),
		C(3df9b04434771542), C(feddce785ccb661f), C(a644aff716928297), C(dd46aee73824b4ed),
		C(bf8d71879da29b02), C(fc82dccbfc8022a0), C(31bfcd0d9f48d1d3), C(c64ee24d0e7b5f8b),
		C(72c39bea)
	},
	{
		C(ed8b7a4b34954ff7), C(56432de31f4ee757), C(85bd3abaa572b155), C(7d2c38a926dc1b88), C(5245b9eb4cd6791d), C(fb53ab03b9ad0855), C(3664026c8fc669d7),
		C(7d2c38a926dc1b88), C(5245b9eb4cd6791d), C(fb53ab03b9ad0855), C(3664026c8fc669d7),
		C(45024d5080bc196), C(b236ebec2cc2740), C(27231ad0e3443be4), C(145780b63f809250),
		C(a65aa25c)
	},
	{
		C(5d28b43694176c26), C(714cc8bc12d060ae), C(3437726273a83fe6), C(864b1b28ec16ea86), C(6a78a5a4039ec2b9), C(8e959533e35a766), C(347b7c22b75ae65f),
		C(864b1b28ec16ea86), C(6a78a5a4039ec2b9), C(8e959533e35a766), C(347b7c22b75ae65f),
		C(5005892bb61e647c), C(fe646519b4a1894d), C(cd801026f74a8a53), C(8713463e9a1ab9ce),
		C(74740539)
	},
	{
		C(6a1ef3639e1d202e), C(919bc1bd145ad928), C(30f3f7e48c28a773), C(2e8c49d7c7aaa527), C(5e2328fc8701db7c), C(89ef1afca81f7de8), C(b1857db11985d296),
		C(2e8c49d7c7aaa527), C(5e2328fc8701db7c), C(89ef1afca81f7de8), C(b1857db11985d296),
		C(17763d695f616115), C(b8f7bf1fcdc8322c), C(cf0c61938ab07a27), C(1122d3e6edb4e866),
		C(c3ae3c26)
	},
	{
		C(159f4d9e0307b111), C(3e17914a5675a0c), C(af849bd425047b51), C(3b69edadf357432b), C(3a2e311c121e6bf2), C(380fad1e288d57e5), C(bf7c7e8ef0e3b83a),
		C(3b69edadf357432b), C(3a2e311c121e6bf2), C(380fad1e288d57e5), C(bf7c7e8ef0e3b83a),
		C(92966d5f4356ae9b), C(2a03fc66c4d6c036), C(2516d8bddb0d5259), C(b3ffe9737ff5090),
		C(f29db8a2)
	},
	{
		C(cc0a840725a7e25b), C(57c69454396e193a), C(976eaf7eee0b4540), C(cd7a46850b95e901), C(c57f7d060dda246f), C(6b9406ead64079bf), C(11b28e20a573b7bd),
		C(cd7a46850b95e901), C(c57f7d060dda246f), C(6b9406ead64079bf), C(11b28e20a573b7bd),
		C(2d6db356e9369ace), C(dc0afe10fba193), C(5cdb10885dbbfce), C(5c700e205782e35a),
		C(1ef4cbf4)
	},
	{
		C(a2b27ee22f63c3f1), C(9ebde0ce1b3976b2), C(2fe6a92a257af308), C(8c1df927a930af59), C(a462f4423c9e384e), C(236542255b2ad8d9), C(595d201a2c19d5bc),
		C(8c1df927a930af59), C(a462f4423c9e384e), C(236542255b2ad8d9), C(595d201a2c19d5bc),
		C(22c87d4604a67f3), C(585a06eb4bc44c4f), C(b4175a7ac7eabcd8), C(a457d3eeba14ab8c),
		C(a9be6c41)
	},
	{
		C(d8f2f234899bcab3), C(b10b037297c3a168), C(debea2c510ceda7f), C(9498fefb890287ce), C(ae68c2be5b1a69a6), C(6189dfba34ed656c), C(91658f95836e5206),
		C(9498fefb890287ce), C(ae68c2be5b1a69a6), C(6189dfba34ed656c), C(91658f95836e5206),
		C(c0bb4fff32aecd4d), C(94125f505a50eef9), C(6ac406e7cfbce5bb), C(344a4b1dcdb7f5d8),
		C(fa31801)
	},
	{
		C(584f28543864844f), C(d7cee9fc2d46f20d), C(a38dca5657387205), C(7a0b6dbab9a14e69), C(c6d0a9d6b0e31ac4), C(a674d85812c7cf6), C(63538c0351049940),
		C(7a0b6dbab9a14e69), C(c6d0a9d6b0e31ac4), C(a674d85812c7cf6), C(63538c0351049940),
		C(9710e5f0bc93d1d), C(c2bea5bd7c54ddd4), C(48739af2bed0d32d), C(ba2c4e09e21fba85),
		C(8331c5d8)
	},
	{
		C(a94be46dd9aa41af), C(a57e5b7723d3f9bd), C(34bf845a52fd2f), C(843b58463c8df0ae), C(74b258324e916045), C(bdd7353230eb2b38), C(fad31fced7abade5),
		C(843b58463c8df0ae), C(74b258324e916045), C(bdd7353230eb2b38), C(fad31fced7abade5),
		C(2436aeafb0046f85), C(65bc9af9e5e33161), C(92733b1b3ae90628), C(f48143eaf78a7a89),
		C(e9876db8)
	},
	{
		C(9a87bea227491d20), C(a468657e2b9c43e7), C(af9ba60db8d89ef7), C(cc76f429ea7a12bb), C(5f30eaf2bb14870a), C(434e824cb3e0cd11), C(431a4d382e39d16e),
		C(cc76f429ea7a12bb), C(5f30eaf2bb14870a), C(434e824cb3e0cd11), C(431a4d382e39d16e),
		C(9e51f913c4773a8), C(32ab1925823d0add), C(99c61b54c1d8f69d), C(38cfb80f02b43b1f),
		C(27b0604e)
	},
	{
		C(27688c24958d1a5c), C(e3b4a1c9429cf253), C(48a95811f70d64bc), C(328063229db22884), C(67e9c95f8ba96028), C(7c6bf01c60436075), C(fa55161e7d9030b2),
		C(328063229db22884), C(67e9c95f8ba96028), C(7c6bf01c60436075), C(fa55161e7d9030b2),
		C(dadbc2f0dab91681), C(da39d7a4934ca11), C(162e845d24c1b45c), C(eb5b9dcd8c6ed31b),
		C(dcec07f2)
	},
	{
		C(5d1d37790a1873ad), C(ed9cd4bcc5fa1090), C(ce51cde05d8cd96a), C(f72c26e624407e66), C(a0eb541bdbc6d409), C(c3f40a2f40b3b213), C(6a784de68794492d),
		C(f72c26e624407e66), C(a0eb541bdbc6d409), C(c3f40a2f40b3b213), C(6a784de68794492d),
		C(10a38a23dbef7937), C(6a5560f853252278), C(c3387bbf3c7b82ba), C(fbee7c12eb072805),
		C(cff0a82a)
	},
	{
		C(1f03fd18b711eea9), C(566d89b1946d381a), C(6e96e83fc92563ab), C(405f66cf8cae1a32), C(d7261740d8f18ce6), C(fea3af64a413d0b2), C(d64d1810e83520fe),
		C(405f66cf8cae1a32), C(d7261740d8f18ce6), C(fea3af64a413d0b2), C(d64d1810e83520fe),
		C(e1334a00a580c6e8), C(454049e1b52c15f), C(8895d823d9778247), C(efa7f2e88b826618),
		C(fec83621)
	},
	{
		C(f0316f286cf527b6), C(f84c29538de1aa5a), C(7612ed3c923d4a71), C(d4eccebe9393ee8a), C(2eb7867c2318cc59), C(1ce621fd700fe396), C(686450d7a346878a),
		C(d4eccebe9393ee8a), C(2eb7867c2318cc59), C(1ce621fd700fe396), C(686450d7a346878a),
		C(75a5f37579f8b4cb), C(500cc16eb6541dc7), C(b7b02317b539d9a6), C(3519ddff5bc20a29),
		C(743d8dc)
	},
	{
		C(297008bcb3e3401d), C(61a8e407f82b0c69), C(a4a35bff0524fa0e), C(7a61d8f552a53442), C(821d1d8d8cfacf35), C(7cc06361b86d0559), C(119b617a8c2be199),
		C(7a61d8f552a53442), C(821d1d8d8cfacf35), C(7cc06361b86d0559), C(119b617a8c2be199),
		C(2996487da6721759), C(61a901376070b91d), C(d88dee12ae9c9b3c), C(5665491be1fa53a7),
		C(64d41d26)
	},
	{
		C(43c6252411ee3be), C(b4ca1b8077777168), C(2746dc3f7da1737f), C(2247a4b2058d1c50), C(1b3fa184b1d7bcc0), C(deb85613995c06ed), C(cbe1d957485a3ccd),
		C(2247a4b2058d1c50), C(1b3fa184b1d7bcc0), C(deb85613995c06ed), C(cbe1d957485a3ccd),
		C(dfe241f8f33c96b6), C(6597eb05019c2109), C(da344b2a63a219cf), C(79b8e3887612378a),
		C(acd90c81)
	},
	{
		C(ce38a9a54fad6599), C(6d6f4a90b9e8755e), C(c3ecc79ff105de3f), C(e8b9ee96efa2d0e), C(90122905c4ab5358), C(84f80c832d71979c), C(229310f3ffbbf4c6),
		C(e8b9ee96efa2d0e), C(90122905c4ab5358), C(84f80c832d71979c), C(229310f3ffbbf4c6),
		C(cc9eb42100cd63a7), C(7a283f2f3da7b9f), C(359b061d314e7a72), C(d0d959720028862),
		C(7c746a4b)
	},
	{
		C(270a9305fef70cf), C(600193999d884f3a), C(f4d49eae09ed8a1), C(2e091b85660f1298), C(bfe37fae1cdd64c9), C(8dddfbab930f6494), C(2ccf4b08f5d417a),
		C(2e091b85660f1298), C(bfe37fae1cdd64c9), C(8dddfbab930f6494), C(2ccf4b08f5d417a),
		C(365c2ee85582fe6), C(dee027bcd36db62a), C(b150994d3c7e5838), C(fdfd1a0e692e436d),
		C(b1047e99)
	},
	{
		C(e71be7c28e84d119), C(eb6ace59932736e6), C(70c4397807ba12c5), C(7a9d77781ac53509), C(4489c3ccfda3b39c), C(fa722d4f243b4964), C(25f15800bffdd122),
		C(7a9d77781ac53509), C(4489c3ccfda3b39c), C(fa722d4f243b4964), C(25f15800bffdd122),
		C(ed85e4157fbd3297), C(aab1967227d59efd), C(2199631212eb3839), C(3e4c19359aae1cc2),
		C(d1fd1068)
	},
	{
		C(b5b58c24b53aaa19), C(d2a6ab0773dd897f), C(ef762fe01ecb5b97), C(9deefbcfa4cab1f1), C(b58f5943cd2492ba), C(a96dcc4d1f4782a7), C(102b62a82309dde5),
		C(9deefbcfa4cab1f1), C(b58f5943cd2492ba), C(a96dcc4d1f4782a7), C(102b62a82309dde5),
		C(35fe52684763b338), C(afe2616651eaad1f), C(43e38715bdfa05e7), C(83c9ba83b5ec4a40),
		C(56486077)
	},
	{
		C(44dd59bd301995cf), C(3ccabd76493ada1a), C(540db4c87d55ef23), C(cfc6d7adda35797), C(14c7d1f32332cf03), C(2d553ffbff3be99d), C(c91c4ee0cb563182),
		C(cfc6d7adda35797), C(14c7d1f32332cf03), C(2d553ffbff3be99d), C(c91c4ee0cb563182),
		C(9aa5e507f49136f0), C(760c5dd1a82c4888), C(beea7e974a1cfb5c), C(640b247774fe4bf7),
		C(6069be80)
	},
	{
		C(b4d4789eb6f2630b), C(bf6973263ce8ef0e), C(d1c75c50844b9d3), C(bce905900c1ec6ea), C(c30f304f4045487d), C(a5c550166b3a142b), C(2f482b4e35327287),
		C(bce905900c1ec6ea), C(c30f304f4045487d), C(a5c550166b3a142b), C(2f482b4e35327287),
		C(15b21ddddf355438), C(496471fa3006bab), C(2a8fd458d06c1a32), C(db91e8ae812f0b8d),
		C(2078359b)
	},
	{
		C(12807833c463737c), C(58e927ea3b3776b4), C(72dd20ef1c2f8ad0), C(910b610de7a967bf), C(801bc862120f6bf5), C(9653efeed5897681), C(f5367ff83e9ebbb3),
		C(910b610de7a967bf), C(801bc862120f6bf5), C(9653efeed5897681), C(f5367ff83e9ebbb3),
		C(cf56d489afd1b0bf), C(c7c793715cae3de8), C(631f91d64abae47c), C(5f1f42fb14a444a2),
		C(9ea21004)
	},
	{
		C(e88419922b87176f), C(bcf32f41a7ddbf6f), C(d6ebefd8085c1a0f), C(d1d44fe99451ef72), C(ec951ba8e51e3545), C(c0ca86b360746e96), C(aa679cc066a8040b),
		C(d1d44fe99451ef72), C(ec951ba8e51e3545), C(c0ca86b360746e96), C(aa679cc066a8040b),
		C(51065861ece6ffc1), C(76777368a2997e11), C(87f278f46731100c), C(bbaa4140bdba4527),
		C(9c9cfe88)
	},
	{
		C(105191e0ec8f7f60), C(5918dbfcca971e79), C(6b285c8a944767b9), C(d3e86ac4f5eccfa4), C(e5399df2b106ca1), C(814aadfacd217f1d), C(2754e3def1c405a9),
		C(d3e86ac4f5eccfa4), C(e5399df2b106ca1), C(814aadfacd217f1d), C(2754e3def1c405a9),
		C(99290323b9f06e74), C(a9782e043f271461), C(13c8b3b8c275a860), C(6038d620e581e9e7),
		C(b70a6ddd)
	},
	{
		C(a5b88bf7399a9f07), C(fca3ddfd96461cc4), C(ebe738fdc0282fc6), C(69afbc800606d0fb), C(6104b97a9db12df7), C(fcc09198bb90bf9f), C(c5e077e41a65ba91),
		C(69afbc800606d0fb), C(6104b97a9db12df7), C(fcc09198bb90bf9f), C(c5e077e41a65ba91),
		C(db261835ee8aa08e), C(db0ee662e5796dc9), C(fc1880ecec499e5f), C(648866fbe1502034),
		C(dea37298)
	},
	{
		C(d08c3f5747d84f50), C(4e708b27d1b6f8ac), C(70f70fd734888606), C(909ae019d761d019), C(368bf4aab1b86ef9), C(308bd616d5460239), C(4fd33269f76783ea),
		C(909ae019d761d019), C(368bf4aab1b86ef9), C(308bd616d5460239), C(4fd33269f76783ea),
		C(7d53b37c19713eab), C(6bba6eabda58a897), C(91abb50efc116047), C(4e902f347e0e0e35),
		C(8f480819)
	},
	{
		C(2f72d12a40044b4b), C(889689352fec53de), C(f03e6ad87eb2f36), C(ef79f28d874b9e2d), C(b512089e8e63b76c), C(24dc06833bf193a9), C(3c23308ba8e99d7e),
		C(ef79f28d874b9e2d), C(b512089e8e63b76c), C(24dc06833bf193a9), C(3c23308ba8e99d7e),
		C(5ceff7b85cacefb7), C(ef390338898cd73), C(b12967d7d2254f54), C(de874cbd8aef7b75),
		C(30b3b16)
	},
	{
		C(aa1f61fdc5c2e11e), C(c2c56cd11277ab27), C(a1e73069fdf1f94f), C(8184bab36bb79df0), C(c81929ce8655b940), C(301b11bf8a4d8ce8), C(73126fd45ab75de9),
		C(8184bab36bb79df0), C(c81929ce8655b940), C(301b11bf8a4d8ce8), C(73126fd45ab75de9),
		C(4bd6f76e4888229a), C(9aae355b54a756d5), C(ca3de9726f6e99d5), C(83f80cac5bc36852),
		C(f31bc4e8)
	},
	{
		C(9489b36fe2246244), C(3355367033be74b8), C(5f57c2277cbce516), C(bc61414f9802ecaf), C(8edd1e7a50562924), C(48f4ab74a35e95f2), C(cc1afcfd99a180e7),
		C(bc61414f9802ecaf), C(8edd1e7a50562924), C(48f4ab74a35e95f2), C(cc1afcfd99a180e7),
		C(517dd5e3acf66110), C(7dd3ad9e8978b30d), C(1f6d5dfc70de812b), C(947daaba6441aaf3),
		C(419f953b)
	},
	{
		C(358d7c0476a044cd), C(e0b7b47bcbd8854f), C(ffb42ec696705519), C(d45e44c263e95c38), C(df61db53923ae3b1), C(f2bc948cc4fc027c), C(8a8000c6066772a3),
		C(d45e44c263e95c38), C(df61db53923ae3b1), C(f2bc948cc4fc027c), C(8a8000c6066772a3),
		C(9fd93c942d31fa17), C(d7651ecebe09cbd3), C(68682cefb6a6f165), C(541eb99a2dcee40e),
		C(20e9e76d)
	},
	{
		C(b0c48df14275265a), C(9da4448975905efa), C(d716618e414ceb6d), C(30e888af70df1e56), C(4bee54bd47274f69), C(178b4059e1a0afe5), C(6e2c96b7f58e5178),
		C(30e888af70df1e56), C(4bee54bd47274f69), C(178b4059e1a0afe5), C(6e2c96b7f58e5178),
		C(bb429d3b9275e9bc), C(c198013f09cafdc6), C(ec0a6ee4fb5de348), C(744e1e8ed2eb1eb0),
		C(646f0ff8)
	},
	{
		C(daa70bb300956588), C(410ea6883a240c6d), C(f5c8239fb5673eb3), C(8b1d7bb4903c105f), C(cfb1c322b73891d4), C(5f3b792b22f07297), C(fd64061f8be86811),
		C(8b1d7bb4903c105f), C(cfb1c322b73891d4), C(5f3b792b22f07297), C(fd64061f8be86811),
		C(1d2db712921cfc2b), C(cd1b2b2f2cee18ae), C(6b6f8790dc7feb09), C(46c179efa3f0f518),
		C(eeb7eca8)
	},
	{
		C(4ec97a20b6c4c7c2), C(5913b1cd454f29fd), C(a9629f9daf06d685), C(852c9499156a8f3), C(3a180a6abfb79016), C(9fc3c4764037c3c9), C(2890c42fc0d972cf),
		C(852c9499156a8f3), C(3a180a6abfb79016), C(9fc3c4764037c3c9), C(2890c42fc0d972cf),
		C(1f92231d4e537651), C(fab8bb07aa54b7b9), C(e05d2d771c485ed4), C(d50b34bf808ca731),
		C(8112bb9)
	},
	{
		C(5c3323628435a2e8), C(1bea45ce9e72a6e3), C(904f0a7027ddb52e), C(939f31de14dcdc7b), C(a68fdf4379df068), C(f169e1f0b835279d), C(7498e432f9619b27),
		C(939f31de14dcdc7b), C(a68fdf4379df068), C(f169e1f0b835279d), C(7498e432f9619b27),
		C(1aa2a1f11088e785), C(d6ad72f45729de78), C(9a63814157c80267), C(55538e35c648e435),
		C(85a6d477)
	},
	{
		C(c1ef26bea260abdb), C(6ee423f2137f9280), C(df2118b946ed0b43), C(11b87fb1b900cc39), C(e33e59b90dd815b1), C(aa6cb5c4bafae741), C(739699951ca8c713),
		C(11b87fb1b900cc39), C(e33e59b90dd815b1), C(aa6cb5c4bafae741), C(739699951ca8c713),
		C(2b4389a967310077), C(1d5382568a31c2c9), C(55d1e787fbe68991), C(277c254bc31301e7),
		C(56f76c84)
	},
	{
		C(6be7381b115d653a), C(ed046190758ea511), C(de6a45ffc3ed1159), C(a64760e4041447d0), C(e3eac49f3e0c5109), C(dd86c4d4cb6258e2), C(efa9857afd046c7f),
		C(a64760e4041447d0), C(e3eac49f3e0c5109), C(dd86c4d4cb6258e2), C(efa9857afd046c7f),
		C(fab793dae8246f16), C(c9e3b121b31d094c), C(a2a0f55858465226), C(dba6f0ff39436344),
		C(9af45d55)
	},
	{
		C(ae3eece1711b2105), C(14fd3f4027f81a4a), C(abb7e45177d151db), C(501f3e9b18861e44), C(465201170074e7d8), C(96d5c91970f2cb12), C(40fd28c43506c95d),
		C(501f3e9b18861e44), C(465201170074e7d8), C(96d5c91970f2cb12), C(40fd28c43506c95d),
		C(e86c4b07802aaff3), C(f317d14112372a70), C(641b13e587711650), C(4915421ab1090eaa),
		C(d1c33760)
	},
	{
		C(376c28588b8fb389), C(6b045e84d8491ed2), C(4e857effb7d4e7dc), C(154dd79fd2f984b4), C(f11171775622c1c3), C(1fbe30982e78e6f0), C(a460a15dcf327e44),
		C(154dd79fd2f984b4), C(f11171775622c1c3), C(1fbe30982e78e6f0), C(a460a15dcf327e44),
		C(f359e0900cc3d582), C(7e11070447976d00), C(324e6daf276ea4b5), C(7aa6e2df0cc94fa2),
		C(c56bbf69)
	},
	{
		C(58d943503bb6748f), C(419c6c8e88ac70f6), C(586760cbf3d3d368), C(b7e164979d5ccfc1), C(12cb4230d26bf286), C(f1bf910d44bd84cb), C(b32c24c6a40272),
		C(b7e164979d5ccfc1), C(12cb4230d26bf286), C(f1bf910d44bd84cb), C(b32c24c6a40272),
		C(11ed12e34c48c039), C(b0c2538e51d0a6ac), C(4269bb773e1d553a), C(e35a9dbabd34867),
		C(abecfb9b)
	},
	{
		C(dfff5989f5cfd9a1), C(bcee2e7ea3a96f83), C(681c7874adb29017), C(3ff6c8ac7c36b63a), C(48bc8831d849e326), C(30b078e76b0214e2), C(42954e6ad721b920),
		C(3ff6c8ac7c36b63a), C(48bc8831d849e326), C(30b078e76b0214e2), C(42954e6ad721b920),
		C(f9aeb33d164b4472), C(7b353b110831dbdc), C(16f64c82f44ae17b), C(b71244cc164b3b2b),
		C(8de13255)
	},
	{
		C(7fb19eb1a496e8f5), C(d49e5dfdb5c0833f), C(c0d5d7b2f7c48dc7), C(1a57313a32f22dde), C(30af46e49850bf8b), C(aa0fe8d12f808f83), C(443e31d70873bb6b),
		C(1a57313a32f22dde), C(30af46e49850bf8b), C(aa0fe8d12f808f83), C(443e31d70873bb6b),
		C(bbeb67c49c9fdc13), C(18f1e2a88f59f9d5), C(fb1b05038e5def11), C(d0450b5ce4c39c52),
		C(a98ee299)
	},
	{
		C(5dba5b0dadccdbaa), C(4ba8da8ded87fcdc), C(f693fdd25badf2f0), C(e9029e6364286587), C(ae69f49ecb46726c), C(18e002679217c405), C(bd6d66e85332ae9f),
		C(e9029e6364286587), C(ae69f49ecb46726c), C(18e002679217c405), C(bd6d66e85332ae9f),
		C(6bf330b1c353dd2a), C(74e9f2e71e3a4152), C(3f85560b50f6c413), C(d33a52a47eaed2b4),
		C(3015f556)
	},
	{
		C(688bef4b135a6829), C(8d31d82abcd54e8e), C(f95f8a30d55036d7), C(3d8c90e27aa2e147), C(2ec937ce0aa236b4), C(89b563996d3a0b78), C(39b02413b23c3f08),
		C(3d8c90e27aa2e147), C(2ec937ce0aa236b4), C(89b563996d3a0b78), C(39b02413b23c3f08),
		C(8d475a2e64faf2d2), C(48567f7dca46ecaf), C(254cda08d5f87a6d), C(ec6ae9f729c47039),
		C(5a430e29)
	},
	{
		C(d8323be05433a412), C(8d48fa2b2b76141d), C(3d346f23978336a5), C(4d50c7537562033f), C(57dc7625b61dfe89), C(9723a9f4c08ad93a), C(5309596f48ab456b),
		C(4d50c7537562033f), C(57dc7625b61dfe89), C(9723a9f4c08ad93a), C(5309596f48ab456b),
		C(7e453088019d220f), C(8776067ba6ab9714), C(67e1d06bd195de39), C(74a1a32f8994b918),
		C(2797add0)
	},
	{
		C(3b5404278a55a7fc), C(23ca0b327c2d0a81), C(a6d65329571c892c), C(45504801e0e6066b), C(86e6c6d6152a3d04), C(4f3db1c53eca2952), C(d24d69b3e9ef10f3),
		C(45504801e0e6066b), C(86e6c6d6152a3d04), C(4f3db1c53eca2952), C(d24d69b3e9ef10f3),
		C(93a0de2219e66a70), C(8932c7115ccb1f8a), C(5ef503fdf2841a8c), C(38064dd9efa80a41),
		C(27d55016)
	},
	{
		C(2a96a3f96c5e9bbc), C(8caf8566e212dda8), C(904de559ca16e45e), C(f13bc2d9c2fe222e), C(be4ccec9a6cdccfd), C(37b2cbdd973a3ac9), C(7b3223cd9c9497be),
		C(f13bc2d9c2fe222e), C(be4ccec9a6cdccfd), C(37b2cbdd973a3ac9), C(7b3223cd9c9497be),
		C(d5904440f376f889), C(62b13187699c473c), C(4751b89251f26726), C(9500d84fa3a61ba8),
		C(84945a82)
	},
	{
		C(22bebfdcc26d18ff), C(4b4d8dcb10807ba1), C(40265eee30c6b896), C(3752b423073b119a), C(377dc5eb7c662bdb), C(2b9f07f93a6c25b9), C(96f24ede2bdc0718),
		C(3752b423073b119a), C(377dc5eb7c662bdb), C(2b9f07f93a6c25b9), C(96f24ede2bdc0718),
		C(f7699b12c31417bd), C(17b366f401c58b2), C(bf60188d5f437b37), C(484436e56df17f04),
		C(3ef7e224)
	},
	{
		C(627a2249ec6bbcc2), C(c0578b462a46735a), C(4974b8ee1c2d4f1f), C(ebdbb918eb6d837f), C(8fb5f218dd84147c), C(c77dd1f881df2c54), C(62eac298ec226dc3),
		C(ebdbb918eb6d837f), C(8fb5f218dd84147c), C(c77dd1f881df2c54), C(62eac298ec226dc3),
		C(43eded83c4b60bd0), C(9a0a403b5487503b), C(25f305d9147f0bda), C(3ad417f511bc1e64),
		C(35ed8dc8)
	},
	{
		C(3abaf1667ba2f3e0), C(ee78476b5eeadc1), C(7e56ac0a6ca4f3f4), C(f1b9b413df9d79ed), C(a7621b6fd02db503), C(d92f7ba9928a4ffe), C(53f56babdcae96a6),
		C(f1b9b413df9d79ed), C(a7621b6fd02db503), C(d92f7ba9928a4ffe), C(53f56babdcae96a6),
		C(5302b89fc48713ab), C(d03e3b04dbe7a2f2), C(fa74ef8af6d376a7), C(103c8cdea1050ef2),
		C(6a75e43d)
	},
	{
		C(3931ac68c5f1b2c9), C(efe3892363ab0fb0), C(40b707268337cd36), C(a53a6b64b1ac85c9), C(d50e7f86ee1b832b), C(7bab08fdd26ba0a4), C(7587743c18fe2475),
		C(a53a6b64b1ac85c9), C(d50e7f86ee1b832b), C(7bab08fdd26ba0a4), C(7587743c18fe2475),
		C(e3b5d5d490cf5761), C(dfc053f7d065edd5), C(42ffd8d5fb70129f), C(599ca38677cccdc3),
		C(235d9805)
	},
	{
		C(b98fb0606f416754), C(46a6e5547ba99c1e), C(c909d82112a8ed2), C(dbfaae9642b3205a), C(f676a1339402bcb9), C(f4f12a5b1ac11f29), C(7db8bad81249dee4),
		C(dbfaae9642b3205a), C(f676a1339402bcb9), C(f4f12a5b1ac11f29), C(7db8bad81249dee4),
		C(b26e46f2da95922e), C(2aaedd5e12e3c611), C(a0e2d9082966074), C(c64da8a167add63d),
		C(f7d69572)
	},
	{
		C(7f7729a33e58fcc4), C(2e4bc1e7a023ead4), C(e707008ea7ca6222), C(47418a71800334a0), C(d10395d8fc64d8a4), C(8257a30062cb66f), C(6786f9b2dc1ff18a),
		C(47418a71800334a0), C(d10395d8fc64d8a4), C(8257a30062cb66f), C(6786f9b2dc1ff18a),
		C(5633f437bb2f180f), C(e5a3a405737d22d6), C(ca0ff1ef6f7f0b74), C(d0ae600684b16df8),
		C(bacd0199)
	},
	{
		C(42a0aa9ce82848b3), C(57232730e6bee175), C(f89bb3f370782031), C(caa33cf9b4f6619c), C(b2c8648ad49c209f), C(9e89ece0712db1c0), C(101d8274a711a54b),
		C(caa33cf9b4f6619c), C(b2c8648ad49c209f), C(9e89ece0712db1c0), C(101d8274a711a54b),
		C(538e79f1e70135cd), C(e1f5a76f983c844e), C(653c082fd66088fc), C(1b9c9b464b654958),
		C(e428f50e)
	},
	{
		C(6b2c6d38408a4889), C(de3ef6f68fb25885), C(20754f456c203361), C(941f5023c0c943f9), C(dfdeb9564fd66f24), C(2140cec706b9d406), C(7b22429b131e9c72),
		C(941f5023c0c943f9), C(dfdeb9564fd66f24), C(2140cec706b9d406), C(7b22429b131e9c72),
		C(94215c22eb940f45), C(d28b9ed474f7249a), C(6f25e88f2fbf9f56), C(b6718f9e605b38ac),
		C(81eaaad3)
	},
	{
		C(930380a3741e862a), C(348d28638dc71658), C(89dedcfd1654ea0d), C(7e7f61684080106), C(837ace9794582976), C(5ac8ca76a357eb1b), C(32b58308625661fb),
		C(7e7f61684080106), C(837ace9794582976), C(5ac8ca76a357eb1b), C(32b58308625661fb),
		C(c09705c4572025d9), C(f9187f6af0291303), C(1c0edd8ee4b02538), C(e6cb105daa0578a),
		C(addbd3e3)
	},
	{
		C(94808b5d2aa25f9a), C(cec72968128195e0), C(d9f4da2bdc1e130f), C(272d8dd74f3006cc), C(ec6c2ad1ec03f554), C(4ad276b249a5d5dd), C(549a22a17c0cde12),
		C(272d8dd74f3006cc), C(ec6c2ad1ec03f554), C(4ad276b249a5d5dd), C(549a22a17c0cde12),
		C(602119cb824d7cde), C(f4d3cef240ef35fa), C(e889895e01911bc7), C(785a7e5ac20e852b),
		C(e66dbca0)
	},
	{
		C(b31abb08ae6e3d38), C(9eb9a95cbd9e8223), C(8019e79b7ee94ea9), C(7b2271a7a3248e22), C(3b4f700e5a0ba523), C(8ebc520c227206fe), C(da3f861490f5d291),
		C(7b2271a7a3248e22), C(3b4f700e5a0ba523), C(8ebc520c227206fe), C(da3f861490f5d291),
		C(d08a689f9f3aa60e), C(547c1b97a068661f), C(4b15a67fa29172f0), C(eaf40c085191d80f),
		C(afe11fd5)
	},
	{
		C(dccb5534a893ea1a), C(ce71c398708c6131), C(fe2396315457c164), C(3f1229f4d0fd96fb), C(33130aa5fa9d43f2), C(e42693d5b34e63ab), C(2f4ef2be67f62104),
		C(3f1229f4d0fd96fb), C(33130aa5fa9d43f2), C(e42693d5b34e63ab), C(2f4ef2be67f62104),
		C(372e5153516e37b9), C(af9ec142ab12cc86), C(777920c09345e359), C(e7c4a383bef8adc6),
		C(a71a406f)
	},
	{
		C(6369163565814de6), C(8feb86fb38d08c2f), C(4976933485cc9a20), C(7d3e82d5ba29a90d), C(d5983cc93a9d126a), C(37e9dfd950e7b692), C(80673be6a7888b87),
		C(7d3e82d5ba29a90d), C(d5983cc93a9d126a), C(37e9dfd950e7b692), C(80673be6a7888b87),
		C(57f732dc600808bc), C(59477199802cc78b), C(f824810eb8f2c2de), C(c4a3437f05b3b61c),
		C(9d90eaf5)
	},
	{
		C(edee4ff253d9f9b3), C(96ef76fb279ef0ad), C(a4d204d179db2460), C(1f3dcdfa513512d6), C(4dc7ec07283117e4), C(4438bae88ae28bf9), C(aa7eae72c9244a0d),
		C(1f3dcdfa513512d6), C(4dc7ec07283117e4), C(4438bae88ae28bf9), C(aa7eae72c9244a0d),
		C(b9aedc8d3ecc72df), C(b75a8eb090a77d62), C(6b15677f9cd91507), C(51d8282cb3a9ddbf),
		C(6665db10)
	},
	{
		C(941993df6e633214), C(929bc1beca5b72c6), C(141fc52b8d55572d), C(b3b782ad308f21ed), C(4f2676485041dee0), C(bfe279aed5cb4bc8), C(2a62508a467a22ff),
		C(b3b782ad308f21ed), C(4f2676485041dee0), C(bfe279aed5cb4bc8), C(2a62508a467a22ff),
		C(e74d29eab742385d), C(56b05cd90ecfc293), C(c603728ea73f8844), C(8638fcd21bc692c4),
		C(9c977cbf)
	},
	{
		C(859838293f64cd4c), C(484403b39d44ad79), C(bf674e64d64b9339), C(44d68afda9568f08), C(478568ed51ca1d65), C(679c204ad3d9e766), C(b28e788878488dc1),
		C(44d68afda9568f08), C(478568ed51ca1d65), C(679c204ad3d9e766), C(b28e788878488dc1),
		C(d001a84d3a84fae6), C(d376958fe4cb913e), C(17435277e36c86f0), C(23657b263c347aa6),
		C(ee83ddd4)
	},
	{
		C(c19b5648e0d9f555), C(328e47b2b7562993), C(e756b92ba4bd6a51), C(c3314e362764ddb8), C(6481c084ee9ec6b5), C(ede23fb9a251771), C(bd617f2643324590),
		C(c3314e362764ddb8), C(6481c084ee9ec6b5), C(ede23fb9a251771), C(bd617f2643324590),
		C(d2d30c9b95e030f5), C(8a517312ffc5795e), C(8b1f325033bd535e), C(3ee6e867e03f2892),
		C(26519cc)
	},
	{
		C(f963b63b9006c248), C(9e9bf727ffaa00bc), C(c73bacc75b917e3a), C(2c6aa706129cc54c), C(17a706f59a49f086), C(c7c1eec455217145), C(6adfdc6e07602d42),
		C(2c6aa706129cc54c), C(17a706f59a49f086), C(c7c1eec455217145), C(6adfdc6e07602d42),
		C(fb75fca30d848dd2), C(5228c9ed14653ed4), C(953958910153b1a2), C(a430103a24f42a5d),
		C(a485a53f)
	},
	{
		C(6a8aa0852a8c1f3b), C(c8f1e5e206a21016), C(2aa554aed1ebb524), C(fc3e3c322cd5d89b), C(b7e3911dc2bd4ebb), C(fcd6da5e5fae833a), C(51ed3c41f87f9118),
		C(fc3e3c322cd5d89b), C(b7e3911dc2bd4ebb), C(fcd6da5e5fae833a), C(51ed3c41f87f9118),
		C(f31750cbc19c420a), C(186dab1abada1d86), C(ca7f88cb894b3cd7), C(2859eeb1c373790c),
		C(f62bc412)
	},
	{
		C(740428b4d45e5fb8), C(4c95a4ce922cb0a5), C(e99c3ba78feae796), C(914f1ea2fdcebf5c), C(9566453c07cd0601), C(9841bf66d0462cd), C(79140c1c18536aeb),
		C(914f1ea2fdcebf5c), C(9566453c07cd0601), C(9841bf66d0462cd), C(79140c1c18536aeb),
		C(a963b930b05820c2), C(6a7d9fa0c8c45153), C(64214c40d07cf39b), C(7057daf1d806c014),
		C(8975a436)
	},
	{
		C(658b883b3a872b86), C(2f0e303f0f64827a), C(975337e23dc45e1), C(99468a917986162b), C(7b31434aac6e0af0), C(f6915c1562c7d82f), C(e4071d82a6dd71db),
		C(99468a917986162b), C(7b31434aac6e0af0), C(f6915c1562c7d82f), C(e4071d82a6dd71db),
		C(5f5331f077b5d996), C(7b314ba21b747a4f), C(5a73cb9521da17f5), C(12ed435fae286d86),
		C(94ff7f41)
	},
	{
		C(6df0a977da5d27d4), C(891dd0e7cb19508), C(fd65434a0b71e680), C(8799e4740e573c50), C(9e739b52d0f341e8), C(cdfd34ba7d7b03eb), C(5061812ce6c88499),
		C(8799e4740e573c50), C(9e739b52d0f341e8), C(cdfd34ba7d7b03eb), C(5061812ce6c88499),
		C(612b8d8f2411dc5c), C(878bd883d29c7787), C(47a846727182bb), C(ec4949508c8b3b9a),
		C(760aa031)
	},
	{
		C(a900275464ae07ef), C(11f2cfda34beb4a3), C(9abf91e5a1c38e4), C(8063d80ab26f3d6d), C(4177b4b9b4f0393f), C(6de42ba8672b9640), C(d0bccdb72c51c18),
		C(8063d80ab26f3d6d), C(4177b4b9b4f0393f), C(6de42ba8672b9640), C(d0bccdb72c51c18),
		C(af3f611b7f22cf12), C(3863c41492645755), C(928c7a616a8f14f9), C(a82c78eb2eadc58b),
		C(3bda76df)
	},
	{
		C(810bc8aa0c40bcb0), C(448a019568d01441), C(f60ec52f60d3aeae), C(52c44837aa6dfc77), C(15d8d8fccdd6dc5b), C(345b793ccfa93055), C(932160fe802ca975),
		C(52c44837aa6dfc77), C(15d8d8fccdd6dc5b), C(345b793ccfa93055), C(932160fe802ca975),
		C(a624b0dd93fc18cd), C(d955b254c2037f1e), C(e540533d370a664c), C(2ba4ec12514e9d7),
		C(498e2e65)
	},
	{
		C(22036327deb59ed7), C(adc05ceb97026a02), C(48bff0654262672b), C(c791b313aba3f258), C(443c7757a4727bee), C(e30e4b2372171bdf), C(f3db986c4156f3cb),
		C(c791b313aba3f258), C(443c7757a4727bee), C(e30e4b2372171bdf), C(f3db986c4156f3cb),
		C(a939aefab97c6e15), C(dbeb8acf1d5b0e6c), C(1e0eab667a795bba), C(80dd539902df4d50),
		C(d38deb48)
	},
	{
		C(7d14dfa9772b00c8), C(595735efc7eeaed7), C(29872854f94c3507), C(bc241579d8348401), C(16dc832804d728f0), C(e9cc71ae64e3f09e), C(bef634bc978bac31),
		C(bc241579d8348401), C(16dc832804d728f0), C(e9cc71ae64e3f09e), C(bef634bc978bac31),
		C(7f64b1fa2a9129e), C(71d831bd530ac7f3), C(c7ad0a8a6d5be6f1), C(82a7d3a815c7aaab),
		C(82b3fb6b)
	},
	{
		C(2d777cddb912675d), C(278d7b10722a13f9), C(f5c02bfb7cc078af), C(4283001239888836), C(f44ca39a6f79db89), C(ed186122d71bcc9f), C(8620017ab5f3ba3b),
		C(4283001239888836), C(f44ca39a6f79db89), C(ed186122d71bcc9f), C(8620017ab5f3ba3b),
		C(e787472187f176c), C(267e64c4728cf181), C(f1ba4b3007c15e30), C(8e3a75d5b02ecfc0),
		C(e500e25f)
	},
	{
		C(f2ec98824e8aa613), C(5eb7e3fb53fe3bed), C(12c22860466e1dd4), C(374dd4288e0b72e5), C(ff8916db706c0df4), C(cb1a9e85de5e4b8d), C(d4d12afb67a27659),
		C(374dd4288e0b72e5), C(ff8916db706c0df4), C(cb1a9e85de5e4b8d), C(d4d12afb67a27659),
		C(feb69095d1ba175a), C(e2003aab23a47fad), C(8163a3ecab894b49), C(46d356674ce041f6),
		C(bd2bb07c)
	},
	{
		C(5e763988e21f487f), C(24189de8065d8dc5), C(d1519d2403b62aa0), C(9136456740119815), C(4d8ff7733b27eb83), C(ea3040bc0c717ef8), C(7617ab400dfadbc),
		C(9136456740119815), C(4d8ff7733b27eb83), C(ea3040bc0c717ef8), C(7617ab400dfadbc),
		C(fb336770c10b17a1), C(6123b68b5b31f151), C(1e147d5f295eccf2), C(9ecbb1333556f977),
		C(3a2b431d)
	},
	{
		C(48949dc327bb96ad), C(e1fd21636c5c50b4), C(3f6eb7f13a8712b4), C(14cf7f02dab0eee8), C(6d01750605e89445), C(4f1cf4006e613b78), C(57c40c4db32bec3b),
		C(14cf7f02dab0eee8), C(6d01750605e89445), C(4f1cf4006e613b78), C(57c40c4db32bec3b),
		C(1fde5a347f4a326e), C(cb5a54308adb0e3f), C(14994b2ba447a23c), C(7067d0abb4257b68),
		C(7322a83d)
	},
	{
		C(b7c4209fb24a85c5), C(b35feb319c79ce10), C(f0d3de191833b922), C(570d62758ddf6397), C(5e0204fb68a7b800), C(4383a9236f8b5a2b), C(7bc1a64641d803a4),
		C(570d62758ddf6397), C(5e0204fb68a7b800), C(4383a9236f8b5a2b), C(7bc1a64641d803a4),
		C(5434d61285099f7a), C(d49449aacdd5dd67), C(97855ba0e9a7d75d), C(da67328062f3a62f),
		C(a645ca1c)
	},
	{
		C(9c9e5be0943d4b05), C(b73dc69e45201cbb), C(aab17180bfe5083d), C(c738a77a9a55f0e2), C(705221addedd81df), C(fd9bd8d397abcfa3), C(8ccf0004aa86b795),
		C(c738a77a9a55f0e2), C(705221addedd81df), C(fd9bd8d397abcfa3), C(8ccf0004aa86b795),
		C(2bb5db2280068206), C(8c22d29f307a01d), C(274a22de02f473c8), C(b8791870f4268182),
		C(8909a45a)
	},
	{
		C(3898bca4dfd6638d), C(f911ff35efef0167), C(24bdf69e5091fc88), C(9b82567ab6560796), C(891b69462b41c224), C(8eccc7e4f3af3b51), C(381e54c3c8f1c7d0),
		C(9b82567ab6560796), C(891b69462b41c224), C(8eccc7e4f3af3b51), C(381e54c3c8f1c7d0),
		C(c80fbc489a558a55), C(1ba88e062a663af7), C(af7b1ef1c0116303), C(bd20e1a5a6b1a0cd),
		C(bd30074c)
	},
	{
		C(5b5d2557400e68e7), C(98d610033574cee), C(dfd08772ce385deb), C(3c13e894365dc6c2), C(26fc7bbcda3f0ef), C(dbb71106cdbfea36), C(785239a742c6d26d),
		C(3c13e894365dc6c2), C(26fc7bbcda3f0ef), C(dbb71106cdbfea36), C(785239a742c6d26d),
		C(f810c415ae05b2f4), C(bb9b9e7398526088), C(70128f1bf830a32b), C(bcc73f82b6410899),
		C(c17cf001)
	},
	{
		C(a927ed8b2bf09bb6), C(606e52f10ae94eca), C(71c2203feb35a9ee), C(6e65ec14a8fb565), C(34bff6f2ee5a7f79), C(2e329a5be2c011b), C(73161c93331b14f9),
		C(6e65ec14a8fb565), C(34bff6f2ee5a7f79), C(2e329a5be2c011b), C(73161c93331b14f9),
		C(15d13f2408aecf88), C(9f5b61b8a4b55b31), C(8fe25a43b296dba6), C(bdad03b7300f284e),
		C(26ffd25a)
	},
	{
		C(8d25746414aedf28), C(34b1629d28b33d3a), C(4d5394aea5f82d7b), C(379f76458a3c8957), C(79dd080f9843af77), C(c46f0a7847f60c1d), C(af1579c5797703cc),
		C(379f76458a3c8957), C(79dd080f9843af77), C(c46f0a7847f60c1d), C(af1579c5797703cc),
		C(8b7d31f338755c14), C(2eff97679512aaa8), C(df07d68e075179ed), C(c8fa6c7a729e7f1f),
		C(f1d8ce3c)
	},
	{
		C(b5bbdb73458712f2), C(1ff887b3c2a35137), C(7f7231f702d0ace9), C(1e6f0910c3d25bd8), C(ad9e250862102467), C(1c842a07abab30cd), C(cd8124176bac01ac),
		C(1e6f0910c3d25bd8), C(ad9e250862102467), C(1c842a07abab30cd), C(cd8124176bac01ac),
		C(ea6ebe7a79b67edc), C(73f598ac9db26713), C(4f4e72d7460b8fc), C(365dc4b9fdf13f21),
		C(3ee8fb17)
	},
	{
		C(3d32a26e3ab9d254), C(fc4070574dc30d3a), C(f02629579c2b27c9), C(b1cf09b0184a4834), C(5c03db48eb6cc159), C(f18c7fcf34d1df47), C(dfb043419ecf1fa9),
		C(b1cf09b0184a4834), C(5c03db48eb6cc159), C(f18c7fcf34d1df47), C(dfb043419ecf1fa9),
		C(dcd78d13f9ca658f), C(4355d408ffe8e49f), C(81eefee908b593b4), C(590c213c20e981a3),
		C(a77acc2a)
	},
	{
		C(9371d3c35fa5e9a5), C(42967cf4d01f30), C(652d1eeae704145c), C(ceaf1a0d15234f15), C(1450a54e45ba9b9), C(65e9c1fd885aa932), C(354d4bc034ba8cbe),
		C(ceaf1a0d15234f15), C(1450a54e45ba9b9), C(65e9c1fd885aa932), C(354d4bc034ba8cbe),
		C(8fd4ff484c08fb4b), C(bf46749866f69ba0), C(cf1c21ede82c9477), C(4217548c43da109),
		C(f4556dee)
	},
	{
		C(cbaa3cb8f64f54e0), C(76c3b48ee5c08417), C(9f7d24e87e61ce9), C(85b8e53f22e19507), C(bb57137739ca486b), C(c77f131cca38f761), C(c56ac3cf275be121),
		C(85b8e53f22e19507), C(bb57137739ca486b), C(c77f131cca38f761), C(c56ac3cf275be121),
		C(9ec1a6c9109d2685), C(3dad0922e76afdb0), C(fd58cbf952958103), C(7b04c908e78639a1),
		C(de287a64)
	},
	{
		C(b2e23e8116c2ba9f), C(7e4d9c0060101151), C(3310da5e5028f367), C(adc52dddb76f6e5e), C(4aad4e925a962b68), C(204b79b7f7168e64), C(df29ed6671c36952),
		C(adc52dddb76f6e5e), C(4aad4e925a962b68), C(204b79b7f7168e64), C(df29ed6671c36952),
		C(e02927cac396d210), C(5d500e71742b638a), C(5c9998af7f27b124), C(3fba9a2573dc2f7),
		C(878e55b9)
	},
	{
		C(8aa77f52d7868eb9), C(4d55bd587584e6e2), C(d2db37041f495f5), C(ce030d15b5fe2f4), C(86b4a7a0780c2431), C(ee070a9ae5b51db7), C(edc293d9595be5d8),
		C(ce030d15b5fe2f4), C(86b4a7a0780c2431), C(ee070a9ae5b51db7), C(edc293d9595be5d8),
		C(3dfc5ec108260a2b), C(8afe28c7123bf4e2), C(da82ef38023a7a5f), C(3e1f77b0174b77c3),
		C(7648486)
	},
	{
		C(858fea922c7fe0c3), C(cfe8326bf733bc6f), C(4e5e2018cf8f7dfc), C(64fd1bc011e5bab7), C(5c9e858728015568), C(97ac42c2b00b29b1), C(7f89caf08c109aee),
		C(64fd1bc011e5bab7), C(5c9e858728015568), C(97ac42c2b00b29b1), C(7f89caf08c109aee),
		C(9a8af34fd0e9dacf), C(bbc54161aa1507e0), C(7cda723ccbbfe5ee), C(2c289d839fb93f58),
		C(57ac0fb1)
	},
	{
		C(46ef25fdec8392b1), C(e48d7b6d42a5cd35), C(56a6fe1c175299ca), C(fdfa836b41dcef62), C(2f8db8030e847e1b), C(5ba0a49ac4f9b0f8), C(dae897ed3e3fce44),
		C(fdfa836b41dcef62), C(2f8db8030e847e1b), C(5ba0a49ac4f9b0f8), C(dae897ed3e3fce44),
		C(9c432e31aef626e7), C(9a36e1c6cd6e3dd), C(5095a167c34d19d), C(a70005cfa6babbea),
		C(d01967ca)
	},
	{
		C(8d078f726b2df464), C(b50ee71cdcabb299), C(f4af300106f9c7ba), C(7d222caae025158a), C(cc028d5fd40241b9), C(dd42515b639e6f97), C(e08e86531a58f87f),
		C(7d222caae025158a), C(cc028d5fd40241b9), C(dd42515b639e6f97), C(e08e86531a58f87f),
		C(d93612c835b37d7b), C(91dd61729b2fa7f4), C(ba765a1bdda09db7), C(55258b451b2b1297),
		C(96ecdf74)
	},
	{
		C(35ea86e6960ca950), C(34fe1fe234fc5c76), C(a00207a3dc2a72b7), C(80395e48739e1a67), C(74a67d8f7f43c3d7), C(dd2bdd1d62246c6e), C(a1f44298ba80acf6),
		C(80395e48739e1a67), C(74a67d8f7f43c3d7), C(dd2bdd1d62246c6e), C(a1f44298ba80acf6),
		C(ad86d86c187bf38), C(26feea1f2eee240d), C(ed7f1fd066b23897), C(a768cf1e0fbb502),
		C(779f5506)
	},
	{
		C(8aee9edbc15dd011), C(51f5839dc8462695), C(b2213e17c37dca2d), C(133b299a939745c5), C(796e2aac053f52b3), C(e8d9fe1521a4a222), C(819a8863e5d1c290),
		C(133b299a939745c5), C(796e2aac053f52b3), C(e8d9fe1521a4a222), C(819a8863e5d1c290),
		C(c0737f0fe34d36ad), C(e6d6d4a267a5cc31), C(98300a7911674c23), C(bef189661c257098),
		C(3c94c2de)
	},
	{
		C(c3e142ba98432dda), C(911d060cab126188), C(b753fbfa8365b844), C(fd1a9ba5e71b08a2), C(7ac0dc2ed7778533), C(b543161ff177188a), C(492fc08a6186f3f4),
		C(fd1a9ba5e71b08a2), C(7ac0dc2ed7778533), C(b543161ff177188a), C(492fc08a6186f3f4),
		C(fc4745f516afd3b6), C(88c30370a53080e), C(65a1bb34abc465e2), C(abbd14662911c8b3),
		C(39f98faf)
	},
	{
		C(123ba6b99c8cd8db), C(448e582672ee07c4), C(cebe379292db9e65), C(938f5bbab544d3d6), C(d2a95f9f2d376d73), C(68b2f16149e81aa3), C(ad7e32f82d86c79d),
		C(938f5bbab544d3d6), C(d2a95f9f2d376d73), C(68b2f16149e81aa3), C(ad7e32f82d86c79d),
		C(4574015ae8626ce2), C(455aa6137386a582), C(658ad2542e8ec20), C(e31d7be2ca35d00),
		C(7af31199)
	},
	{
		C(ba87acef79d14f53), C(b3e0fcae63a11558), C(d5ac313a593a9f45), C(eea5f5a9f74af591), C(578710bcc36fbea2), C(7a8393432188931d), C(705cfc5ec7cc172),
		C(eea5f5a9f74af591), C(578710bcc36fbea2), C(7a8393432188931d), C(705cfc5ec7cc172),
		C(da85ebe5fc427976), C(bfa5c7a454df54c8), C(4632b72a81bf66d2), C(5dd72877db539ee2),
		C(e341a9d6)
	},
	{
		C(bcd3957d5717dc3), C(2da746741b03a007), C(873816f4b1ece472), C(2b826f1a2c08c289), C(da50f56863b55e74), C(b18712f6b3eed83b), C(bdc7cc05ab4c685f),
		C(2b826f1a2c08c289), C(da50f56863b55e74), C(b18712f6b3eed83b), C(bdc7cc05ab4c685f),
		C(9e45fb833d1b0af), C(d7213081db29d82e), C(d2a6b6c6a09ed55e), C(98a7686cba323ca9),
		C(ca24aeeb)
	},
	{
		C(61442ff55609168e), C(6447c5fc76e8c9cf), C(6a846de83ae15728), C(effc2663cffc777f), C(93214f8f463afbed), C(a156ef06066f4e4e), C(a407b6ed8769d51e),
		C(effc2663cffc777f), C(93214f8f463afbed), C(a156ef06066f4e4e), C(a407b6ed8769d51e),
		C(bb2f9ed29745c02a), C(981eecd435b36ad9), C(461a5a05fb9cdff4), C(bd6cb2a87b9f910c),
		C(b2252b57)
	},
	{
		C(dbe4b1b2d174757f), C(506512da18712656), C(6857f3e0b8dd95f), C(5a4fc2728a9bb671), C(ebb971522ec38759), C(1a5a093e6cf1f72b), C(729b057fe784f504),
		C(5a4fc2728a9bb671), C(ebb971522ec38759), C(1a5a093e6cf1f72b), C(729b057fe784f504),
		C(71fcbf42a767f9cf), C(114cfe772da6cdd), C(60cdf9cb629d9d7a), C(e270d10ad088b24e),
		C(72c81da1)
	},
	{
		C(531e8e77b363161c), C(eece0b43e2dae030), C(8294b82c78f34ed1), C(e777b1fd580582f2), C(7b880f58da112699), C(562c6b189a6333f4), C(139d64f88a611d4),
		C(e777b1fd580582f2), C(7b880f58da112699), C(562c6b189a6333f4), C(139d64f88a611d4),
		C(53d8ef17eda64fa4), C(bf3eded14dc60a04), C(2b5c559cf5ec07c5), C(8895f7339d03a48a),
		C(6b9fce95)
	},
	{
		C(f71e9c926d711e2b), C(d77af2853a4ceaa1), C(9aa0d6d76a36fae7), C(dd16cd0fbc08393), C(29a414a5d8c58962), C(72793d8d1022b5b2), C(2e8e69cf7cbffdf0),
		C(dd16cd0fbc08393), C(29a414a5d8c58962), C(72793d8d1022b5b2), C(2e8e69cf7cbffdf0),
		C(3721c0473aa99c9a), C(1cff4ed9c31cd91c), C(4990735033cc482b), C(7fdf8c701c72f577),
		C(19399857)
	},
	{
		C(cb20ac28f52df368), C(e6705ee7880996de), C(9b665cc3ec6972f2), C(4260e8c254e9924b), C(f197a6eb4591572d), C(8e867ff0fb7ab27c), C(f95502fb503efaf3),
		C(4260e8c254e9924b), C(f197a6eb4591572d), C(8e867ff0fb7ab27c), C(f95502fb503efaf3),
		C(30c41876b08e3e22), C(958e2419e3cd22f4), C(f0f3aa1fe119a107), C(481662310a379100),
		C(3c57a994)
	},
	{
		C(e4a794b4acb94b55), C(89795358057b661b), C(9c4cdcec176d7a70), C(4890a83ee435bc8b), C(d8c1c00fceb00914), C(9e7111ba234f900f), C(eb8dbab364d8b604),
		C(4890a83ee435bc8b), C(d8c1c00fceb00914), C(9e7111ba234f900f), C(eb8dbab364d8b604),
		C(b3261452963eebb), C(6cf94b02792c4f95), C(d88fa815ef1e8fc), C(2d687af66604c73),
		C(c053e729)
	},
	{
		C(cb942e91443e7208), C(e335de8125567c2a), C(d4d74d268b86df1f), C(8ba0fdd2ffc8b239), C(f413b366c1ffe02f), C(c05b2717c59a8a28), C(981188eab4fcc8fb),
		C(8ba0fdd2ffc8b239), C(f413b366c1ffe02f), C(c05b2717c59a8a28), C(981188eab4fcc8fb),
		C(e563f49a1d9072ba), C(3c6a3aa4a26367dc), C(ba0db13448653f34), C(31065d756074d7d6),
		C(51cbbba7)
	},
	{
		C(ecca7563c203f7ba), C(177ae2423ef34bb2), C(f60b7243400c5731), C(cf1edbfe7330e94e), C(881945906bcb3cc6), C(4acf0293244855da), C(65ae042c1c2a28c2),
		C(cf1edbfe7330e94e), C(881945906bcb3cc6), C(4acf0293244855da), C(65ae042c1c2a28c2),
		C(b25fa0a1cab33559), C(d98e8daa28124131), C(fce17f50b9c351b3), C(3f995ccf7386864b),
		C(1acde79a)
	},
	{
		C(1652cb940177c8b5), C(8c4fe7d85d2a6d6d), C(f6216ad097e54e72), C(f6521b912b368ae6), C(a9fe4eff81d03e73), C(d6f623629f80d1a3), C(2b9604f32cb7dc34),
		C(f6521b912b368ae6), C(a9fe4eff81d03e73), C(d6f623629f80d1a3), C(2b9604f32cb7dc34),
		C(2a43d84dcf59c7e2), C(d0a197c70c5dae0b), C(6e84d4bbc71d76a0), C(c7e94620378c6cb2),
		C(2d160d13)
	},
	{
		C(31fed0fc04c13ce8), C(3d5d03dbf7ff240a), C(727c5c9b51581203), C(6b5ffc1f54fecb29), C(a8e8e7ad5b9a21d9), C(c4d5a32cd6aac22d), C(d7e274ad22d4a79a),
		C(6b5ffc1f54fecb29), C(a8e8e7ad5b9a21d9), C(c4d5a32cd6aac22d), C(d7e274ad22d4a79a),
		C(368841ea5731a112), C(feaf7bc2e73ca48f), C(636fb272e9ea1f6), C(5d9cb7580c3f6207),
		C(787f5801)
	},
	{
		C(e7b668947590b9b3), C(baa41ad32938d3fa), C(abcbc8d4ca4b39e4), C(381ee1b7ea534f4e), C(da3759828e3de429), C(3e015d76729f9955), C(cbbec51a6485fbde),
		C(381ee1b7ea534f4e), C(da3759828e3de429), C(3e015d76729f9955), C(cbbec51a6485fbde),
		C(9b86605281f20727), C(fc6fcf508676982a), C(3b135f7a813a1040), C(d3a4706bea1db9c9),
		C(c9629828)
	},
	{
		C(1de2119923e8ef3c), C(6ab27c096cf2fe14), C(8c3658edca958891), C(4cc8ed3ada5f0f2), C(4a496b77c1f1c04e), C(9085b0a862084201), C(a1894bde9e3dee21),
		C(4cc8ed3ada5f0f2), C(4a496b77c1f1c04e), C(9085b0a862084201), C(a1894bde9e3dee21),
		C(367fb472dc5b277d), C(7d39ccca16fc6745), C(763f988d70db9106), C(a8b66f7fecb70f02),
		C(be139231)
	},
	{
		C(1269df1e69e14fa7), C(992f9d58ac5041b7), C(e97fcf695a7cbbb4), C(e5d0549802d15008), C(424c134ecd0db834), C(6fc44fd91be15c6c), C(a1a5ef95d50e537d),
		C(e5d0549802d15008), C(424c134ecd0db834), C(6fc44fd91be15c6c), C(a1a5ef95d50e537d),
		C(d1e3daf5d05f5308), C(4c7f81600eaa1327), C(109d1b8d1f9d0d2b), C(871e8699e0aeb862),
		C(7df699ef)
	},
	{
		C(820826d7aba567ff), C(1f73d28e036a52f3), C(41c4c5a73f3b0893), C(aa0d74d4a98db89b), C(36fd486d07c56e1d), C(d0ad23cbb6660d8a), C(1264a84665b35e19),
		C(aa0d74d4a98db89b), C(36fd486d07c56e1d), C(d0ad23cbb6660d8a), C(1264a84665b35e19),
		C(789682bf7d781b33), C(6bfa6abd2fb5722d), C(6779cb3623d33900), C(435ca5214e1ee5f0),
		C(8ce6b96d)
	},
	{
		C(ffe0547e4923cef9), C(3534ed49b9da5b02), C(548a273700fba03d), C(28ac84ca70958f7e), C(d8ae575a68faa731), C(2aaaee9b9dcffd4c), C(6c7faab5c285c6da),
		C(28ac84ca70958f7e), C(d8ae575a68faa731), C(2aaaee9b9dcffd4c), C(6c7faab5c285c6da),
		C(45d94235f99ba78f), C(ab5ea16f39497f5b), C(fb4d6c86fccbdca3), C(8104e6310a5fd2c7),
		C(6f9ed99c)
	},
	{
		C(72da8d1b11d8bc8b), C(ba94b56b91b681c6), C(4e8cc51bd9b0fc8c), C(43505ed133be672a), C(e8f2f9d973c2774e), C(677b9b9c7cad6d97), C(4e1f5d56ef17b906),
		C(43505ed133be672a), C(e8f2f9d973c2774e), C(677b9b9c7cad6d97), C(4e1f5d56ef17b906),
		C(eea3a6038f983767), C(87109f077f86db01), C(ecc1ca41f74d61cc), C(34a87e86e83bed17),
		C(e0244796)
	},
	{
		C(d62ab4e3f88fc797), C(ea86c7aeb6283ae4), C(b5b93e09a7fe465), C(4344a1a0134afe2), C(ff5c17f02b62341d), C(3214c6a587ce4644), C(a905e7ed0629d05c),
		C(4344a1a0134afe2), C(ff5c17f02b62341d), C(3214c6a587ce4644), C(a905e7ed0629d05c),
		C(b5c72690cd716e82), C(7c6097649e6ebe7b), C(7ceee8c6e56a4dcd), C(80ca849dc53eb9e4),
		C(4ccf7e75)
	},
	{
		C(d0f06c28c7b36823), C(1008cb0874de4bb8), C(d6c7ff816c7a737b), C(489b697fe30aa65f), C(4da0fb621fdc7817), C(dc43583b82c58107), C(4b0261debdec3cd6),
		C(489b697fe30aa65f), C(4da0fb621fdc7817), C(dc43583b82c58107), C(4b0261debdec3cd6),
		C(a9748d7b6c0e016c), C(7e8828f7ba4b034b), C(da0fa54348a2512a), C(ebf9745c0962f9ad),
		C(915cef86)
	},
	{
		C(99b7042460d72ec6), C(2a53e5e2b8e795c2), C(53a78132d9e1b3e3), C(c043e67e6fc64118), C(ff0abfe926d844d3), C(f2a9fe5db2e910fe), C(ce352cdc84a964dd),
		C(c043e67e6fc64118), C(ff0abfe926d844d3), C(f2a9fe5db2e910fe), C(ce352cdc84a964dd),
		C(b89bc028aa5e6063), C(a354e7fdac04459c), C(68d6547e6e980189), C(c968dddfd573773e),
		C(5cb59482)
	},
	{
		C(4f4dfcfc0ec2bae5), C(841233148268a1b8), C(9248a76ab8be0d3), C(334c5a25b5903a8c), C(4c94fef443122128), C(743e7d8454655c40), C(1ab1e6d1452ae2cd),
		C(334c5a25b5903a8c), C(4c94fef443122128), C(743e7d8454655c40), C(1ab1e6d1452ae2cd),
		C(fec766de4a8e476c), C(cc0929da9567e71b), C(5f9ef5b5f150c35a), C(87659cabd649768f),
		C(6ca3f532)
	},
	{
		C(fe86bf9d4422b9ae), C(ebce89c90641ef9c), C(1c84e2292c0b5659), C(8bde625a10a8c50d), C(eb8271ded1f79a0b), C(14dc6844f0de7a3c), C(f85b2f9541e7e6da),
		C(8bde625a10a8c50d), C(eb8271ded1f79a0b), C(14dc6844f0de7a3c), C(f85b2f9541e7e6da),
		C(2fe22cfd1683b961), C(ea1d75c5b7aa01ca), C(9eef60a44876bb95), C(950c818e505c6f7f),
		C(e24f3859)
	},
	{
		C(a90d81060932dbb0), C(8acfaa88c5fbe92b), C(7c6f3447e90f7f3f), C(dd52fc14c8dd3143), C(1bc7508516e40628), C(3059730266ade626), C(ffa526822f391c2),
		C(dd52fc14c8dd3143), C(1bc7508516e40628), C(3059730266ade626), C(ffa526822f391c2),
		C(e25232d7afc8a406), C(d2b8a5a3f3b5f670), C(6630f33edb7dfe32), C(c71250ba68c4ea86),
		C(adf5a9c7)
	},
	{
		C(17938a1b0e7f5952), C(22cadd2f56f8a4be), C(84b0d1183d5ed7c1), C(c1336b92fef91bf6), C(80332a3945f33fa9), C(a0f68b86f726ff92), C(a3db5282cf5f4c0b),
		C(c1336b92fef91bf6), C(80332a3945f33fa9), C(a0f68b86f726ff92), C(a3db5282cf5f4c0b),
		C(82640b6fc4916607), C(2dc2a3aa1a894175), C(8b4c852bdee7cc9), C(10b9d0a08b55ff83),
		C(32264b75)
	},
	{
		C(de9e0cb0e16f6e6d), C(238e6283aa4f6594), C(4fb9c914c2f0a13b), C(497cb912b670f3b), C(d963a3f02ff4a5b6), C(4fccefae11b50391), C(42ba47db3f7672f),
		C(497cb912b670f3b), C(d963a3f02ff4a5b6), C(4fccefae11b50391), C(42ba47db3f7672f),
		C(1d6b655a1889feef), C(5f319abf8fafa19f), C(715c2e49deb14620), C(8d9153082ecdcea4),
		C(a64b3376)
	},
	{
		C(6d4b876d9b146d1a), C(aab2d64ce8f26739), C(d315f93600e83fe5), C(2fe9fabdbe7fdd4), C(755db249a2d81a69), C(f27929f360446d71), C(79a1bf957c0c1b92),
		C(2fe9fabdbe7fdd4), C(755db249a2d81a69), C(f27929f360446d71), C(79a1bf957c0c1b92),
		C(3c8a28d4c936c9cd), C(df0d3d13b2c6a902), C(c76702dd97cd2edd), C(1aa220f7be16517),
		C(d33890e)
	},
	{
		C(e698fa3f54e6ea22), C(bd28e20e7455358c), C(9ace161f6ea76e66), C(d53fb7e3c93a9e4), C(737ae71b051bf108), C(7ac71feb84c2df42), C(3d8075cd293a15b4),
		C(d53fb7e3c93a9e4), C(737ae71b051bf108), C(7ac71feb84c2df42), C(3d8075cd293a15b4),
		C(bf8cee5e095d8a7c), C(e7086b3c7608143a), C(e55b0c2fa938d70c), C(fffb5f58e643649c),
		C(926d4b63)
	},
	{
		C(7bc0deed4fb349f7), C(1771aff25dc722fa), C(19ff0644d9681917), C(cf7d7f25bd70cd2c), C(9464ed9baeb41b4f), C(b9064f5c3cb11b71), C(237e39229b012b20),
		C(cf7d7f25bd70cd2c), C(9464ed9baeb41b4f), C(b9064f5c3cb11b71), C(237e39229b012b20),
		C(dd54d3f5d982dffe), C(7fc7562dbfc81dbf), C(5b0dd1924f70945), C(f1760537d8261135),
		C(d51ba539)
	},
	{
		C(db4b15e88533f622), C(256d6d2419b41ce9), C(9d7c5378396765d5), C(9040e5b936b8661b), C(276e08fa53ac27fd), C(8c944d39c2bdd2cc), C(e2514c9802a5743c),
		C(9040e5b936b8661b), C(276e08fa53ac27fd), C(8c944d39c2bdd2cc), C(e2514c9802a5743c),
		C(e82107b11ac90386), C(7d6a22bc35055e6), C(fd6ea9d1c438d8ae), C(be6015149e981553),
		C(7f37636d)
	},
	{
		C(922834735e86ecb2), C(363382685b88328e), C(e9c92960d7144630), C(8431b1bfd0a2379c), C(90383913aea283f9), C(a6163831eb4924d2), C(5f3921b4f9084aee),
		C(8431b1bfd0a2379c), C(90383913aea283f9), C(a6163831eb4924d2), C(5f3921b4f9084aee),
		C(7a70061a1473e579), C(5b19d80dcd2c6331), C(6196b97931faad27), C(869bf6828e237c3f),
		C(b98026c0)
	},
	{
		C(30f1d72c812f1eb8), C(b567cd4a69cd8989), C(820b6c992a51f0bc), C(c54677a80367125e), C(3204fbdba462e606), C(8563278afc9eae69), C(262147dd4bf7e566),
		C(c54677a80367125e), C(3204fbdba462e606), C(8563278afc9eae69), C(262147dd4bf7e566),
		C(2178b63e7ee2d230), C(e9c61ad81f5bff26), C(9af7a81b3c501eca), C(44104a3859f0238f),
		C(b877767e)
	},
	{
		C(168884267f3817e9), C(5b376e050f637645), C(1c18314abd34497a), C(9598f6ab0683fcc2), C(1c805abf7b80e1ee), C(dec9ac42ee0d0f32), C(8cd72e3912d24663),
		C(9598f6ab0683fcc2), C(1c805abf7b80e1ee), C(dec9ac42ee0d0f32), C(8cd72e3912d24663),
		C(1f025d405f1c1d87), C(bf7b6221e1668f8f), C(52316f64e692dbb0), C(7bf43df61ec51b39),
		C(aefae77)
	},
	{
		C(82e78596ee3e56a7), C(25697d9c87f30d98), C(7600a8342834924d), C(6ba372f4b7ab268b), C(8c3237cf1fe243df), C(3833fc51012903df), C(8e31310108c5683f),
		C(6ba372f4b7ab268b), C(8c3237cf1fe243df), C(3833fc51012903df), C(8e31310108c5683f),
		C(126593715c2de429), C(48ca8f35a3f54b90), C(b9322b632f4f8b0), C(926bb169b7337693),
		C(f686911)
	},
	{
		C(aa2d6cf22e3cc252), C(9b4dec4f5e179f16), C(76fb0fba1d99a99a), C(9a62af3dbba140da), C(27857ea044e9dfc1), C(33abce9da2272647), C(b22a7993aaf32556),
		C(9a62af3dbba140da), C(27857ea044e9dfc1), C(33abce9da2272647), C(b22a7993aaf32556),
		C(bf8f88f8019bedf0), C(ed2d7f01fb273905), C(6b45f15901b481cd), C(f88ebb413ba6a8d5),
		C(3deadf12)
	},
	{
		C(7bf5ffd7f69385c7), C(fc077b1d8bc82879), C(9c04e36f9ed83a24), C(82065c62e6582188), C(8ef787fd356f5e43), C(2922e53e36e17dfa), C(9805f223d385010b),
		C(82065c62e6582188), C(8ef787fd356f5e43), C(2922e53e36e17dfa), C(9805f223d385010b),
		C(692154f3491b787d), C(e7e64700e414fbf), C(757d4d4ab65069a0), C(cd029446a8e348e2),
		C(ccf02a4e)
	},
	{
		C(e89c8ff9f9c6e34b), C(f54c0f669a49f6c4), C(fc3e46f5d846adef), C(22f2aa3df2221cc), C(f66fea90f5d62174), C(b75defaeaa1dd2a7), C(9b994cd9a7214fd5),
		C(22f2aa3df2221cc), C(f66fea90f5d62174), C(b75defaeaa1dd2a7), C(9b994cd9a7214fd5),
		C(fac675a31804b773), C(98bcb3b820c50fc6), C(e14af64d28cf0885), C(27466fbd2b360eb5),
		C(176c1722)
	},
	{
		C(a18fbcdccd11e1f4), C(8248216751dfd65e), C(40c089f208d89d7c), C(229b79ab69ae97d), C(a87aabc2ec26e582), C(be2b053721eb26d2), C(10febd7f0c3d6fcb),
		C(229b79ab69ae97d), C(a87aabc2ec26e582), C(be2b053721eb26d2), C(10febd7f0c3d6fcb),
		C(9cc5b9b2f6e3bf7b), C(655d8495fe624a86), C(6381a9f3d1f2bd7e), C(79ebabbfc25c83e2),
		C(26f82ad)
	},
	{
		C(2d54f40cc4088b17), C(59d15633b0cd1399), C(a8cc04bb1bffd15b), C(d332cdb073d8dc46), C(272c56466868cb46), C(7e7fcbe35ca6c3f3), C(ee8f51e5a70399d4),
		C(d332cdb073d8dc46), C(272c56466868cb46), C(7e7fcbe35ca6c3f3), C(ee8f51e5a70399d4),
		C(16737a9c7581fe7b), C(ed04bf52f4b75dcb), C(9707ffb36bd30c1a), C(1390f236fdc0de3e),
		C(b5244f42)
	},
	{
		C(69276946cb4e87c7), C(62bdbe6183be6fa9), C(3ba9773dac442a1a), C(702e2afc7f5a1825), C(8c49b11ea8151fdc), C(caf3fef61f5a86fa), C(ef0b2ee8649d7272),
		C(702e2afc7f5a1825), C(8c49b11ea8151fdc), C(caf3fef61f5a86fa), C(ef0b2ee8649d7272),
		C(9e34a4e08d9441e1), C(7bdc0cd64d5af533), C(a926b14d99e3d868), C(fca923a17788cce4),
		C(49a689e5)
	},
	{
		C(668174a3f443df1d), C(407299392da1ce86), C(c2a3f7d7f2c5be28), C(a590b202a7a5807b), C(968d2593f7ccb54e), C(9dd8d669e3e95dec), C(ee0cc5dd58b6e93a),
		C(a590b202a7a5807b), C(968d2593f7ccb54e), C(9dd8d669e3e95dec), C(ee0cc5dd58b6e93a),
		C(ac65d5a9466fb483), C(221be538b2c9d806), C(5cbe9441784f9fd9), C(d4c7d5d6e3c122b8),
		C(59fcdd3)
	},
	{
		C(5e29be847bd5046), C(b561c7f19c8f80c3), C(5e5abd5021ccaeaf), C(7432d63888e0c306), C(74bbceeed479cb71), C(6471586599575fdf), C(6a859ad23365cba2),
		C(7432d63888e0c306), C(74bbceeed479cb71), C(6471586599575fdf), C(6a859ad23365cba2),
		C(f9ceec84acd18dcc), C(74a242ff1907437c), C(f70890194e1ee913), C(777dfcb4bb01f0ba),
		C(4f4b04e9)
	},
	{
		C(cd0d79f2164da014), C(4c386bb5c5d6ca0c), C(8e771b03647c3b63), C(69db23875cb0b715), C(ada8dd91504ae37f), C(46bf18dbf045ed6a), C(e1b5f67b0645ab63),
		C(69db23875cb0b715), C(ada8dd91504ae37f), C(46bf18dbf045ed6a), C(e1b5f67b0645ab63),
		C(877be8f5dcddff4), C(6d471b5f9ca2e2d1), C(802c86d6f495b9bb), C(a1f9b9b22b3be704),
		C(8b00f891)
	},
	{
		C(e0e6fc0b1628af1d), C(29be5fb4c27a2949), C(1c3f781a604d3630), C(c4af7faf883033aa), C(9bd296c4e9453cac), C(ca45426c1f7e33f9), C(a6bbdcf7074d40c5),
		C(c4af7faf883033aa), C(9bd296c4e9453cac), C(ca45426c1f7e33f9), C(a6bbdcf7074d40c5),
		C(e13a005d7142733b), C(c02b7925c5eeefaf), C(d39119a60441e2d5), C(3c24c710df8f4d43),
		C(16e114f3)
	},
	{
		C(2058927664adfd93), C(6e8f968c7963baa5), C(af3dced6fff7c394), C(42e34cf3d53c7876), C(9cddbb26424dc5e), C(64f6340a6d8eddad), C(2196e488eb2a3a4b),
		C(42e34cf3d53c7876), C(9cddbb26424dc5e), C(64f6340a6d8eddad), C(2196e488eb2a3a4b),
		C(c9e9da25911a16fd), C(e21b4683f3e196a8), C(cb80bf1a4c6fdbb4), C(53792e9b3c3e67f8),
		C(d6b6dadc)
	},
	{
		C(dc107285fd8e1af7), C(a8641a0609321f3f), C(db06e89ffdc54466), C(bcc7a81ed5432429), C(b6d7bdc6ad2e81f1), C(93605ec471aa37db), C(a2a73f8a85a8e397),
		C(bcc7a81ed5432429), C(b6d7bdc6ad2e81f1), C(93605ec471aa37db), C(a2a73f8a85a8e397),
		C(10a012b8ca7ac24b), C(aac5fd63351595cf), C(5bb4c648a226dea0), C(9d11ecb2b5c05c5f),
		C(897e20ac)
	},
	{
		C(fbba1afe2e3280f1), C(755a5f392f07fce), C(9e44a9a15402809a), C(6226a32e25099848), C(ea895661ecf53004), C(4d7e0158db2228b9), C(e5a7d82922f69842),
		C(6226a32e25099848), C(ea895661ecf53004), C(4d7e0158db2228b9), C(e5a7d82922f69842),
		C(2cea7713b69840ca), C(18de7b9ae938375b), C(f127cca08f3cc665), C(b1c22d727665ad2),
		C(f996e05d)
	},
	{
		C(bfa10785ddc1011b), C(b6e1c4d2f670f7de), C(517d95604e4fcc1f), C(ca6552a0dfb82c73), C(b024cdf09e34ba07), C(66cd8c5a95d7393b), C(e3939acf790d4a74),
		C(ca6552a0dfb82c73), C(b024cdf09e34ba07), C(66cd8c5a95d7393b), C(e3939acf790d4a74),
		C(97827541a1ef051e), C(ac2fce47ebe6500c), C(b3f06d3bddf3bd6a), C(1d74afb25e1ce5fe),
		C(c4306af6)
	},
	{
		C(534cc35f0ee1eb4e), C(b703820f1f3b3dce), C(884aa164cf22363), C(f14ef7f47d8a57a3), C(80d1f86f2e061d7c), C(401d6c2f151b5a62), C(e988460224108944),
		C(f14ef7f47d8a57a3), C(80d1f86f2e061d7c), C(401d6c2f151b5a62), C(e988460224108944),
		C(7804d4135f68cd19), C(5487b4b39e69fe8e), C(8cc5999015358a27), C(8f3729b61c2d5601),
		C(6dcad433)
	},
	{
		C(7ca6e3933995dac), C(fd118c77daa8188), C(3aceb7b5e7da6545), C(c8389799445480db), C(5389f5df8aacd50d), C(d136581f22fab5f), C(c2f31f85991da417),
		C(c8389799445480db), C(5389f5df8aacd50d), C(d136581f22fab5f), C(c2f31f85991da417),
		C(aefbf9ff84035a43), C(8accbaf44adadd7c), C(e57f3657344b67f5), C(21490e5e8abdec51),
		C(3c07374d)
	},
	{
		C(f0d6044f6efd7598), C(e044d6ba4369856e), C(91968e4f8c8a1a4c), C(70bd1968996bffc2), C(4c613de5d8ab32ac), C(fe1f4f97206f79d8), C(ac0434f2c4e213a9),
		C(70bd1968996bffc2), C(4c613de5d8ab32ac), C(fe1f4f97206f79d8), C(ac0434f2c4e213a9),
		C(7490e9d82cfe22ca), C(5fbbf7f987454238), C(c39e0dc8368ce949), C(22201d3894676c71),
		C(f0f4602c)
	},
	{
		C(3d69e52049879d61), C(76610636ea9f74fe), C(e9bf5602f89310c0), C(8eeb177a86053c11), C(e390122c345f34a2), C(1e30e47afbaaf8d6), C(7b892f68e5f91732),
		C(8eeb177a86053c11), C(e390122c345f34a2), C(1e30e47afbaaf8d6), C(7b892f68e5f91732),
		C(b87922525fa44158), C(f440a1ee1a1a766b), C(ee8efad279d08c5c), C(421f910c5b60216e),
		C(3e1ea071)
	},
	{
		C(79da242a16acae31), C(183c5f438e29d40), C(6d351710ae92f3de), C(27233b28b5b11e9b), C(c7dfe8988a942700), C(570ed11c4abad984), C(4b4c04632f48311a),
		C(27233b28b5b11e9b), C(c7dfe8988a942700), C(570ed11c4abad984), C(4b4c04632f48311a),
		C(12f33235442cbf9), C(a35315ca0b5b8cdb), C(d8abde62ead5506b), C(fc0fcf8478ad5266),
		C(67580f0c)
	},
	{
		C(461c82656a74fb57), C(d84b491b275aa0f7), C(8f262cb29a6eb8b2), C(49fa3070bc7b06d0), C(f12ed446bd0c0539), C(6d43ac5d1dd4b240), C(7609524fe90bec93),
		C(49fa3070bc7b06d0), C(f12ed446bd0c0539), C(6d43ac5d1dd4b240), C(7609524fe90bec93),
		C(391c2b2e076ec241), C(f5e62deda7839f7b), C(3c7b3186a10d870f), C(77ef4f2cba4f1005),
		C(4e109454)
	},
	{
		C(53c1a66d0b13003), C(731f060e6fe797fc), C(daa56811791371e3), C(57466046cf6896ed), C(8ac37e0e8b25b0c6), C(3e6074b52ad3cf18), C(aa491ce7b45db297),
		C(57466046cf6896ed), C(8ac37e0e8b25b0c6), C(3e6074b52ad3cf18), C(aa491ce7b45db297),
		C(f7a9227c5e5e22c3), C(3d92e0841e29ce28), C(2d30da5b2859e59d), C(ff37fa1c9cbfafc2),
		C(88a474a7)
	},
	{
		C(d3a2efec0f047e9), C(1cabce58853e58ea), C(7a17b2eae3256be4), C(c2dcc9758c910171), C(cb5cddaeff4ddb40), C(5d7cc5869baefef1), C(9644c5853af9cfeb),
		C(c2dcc9758c910171), C(cb5cddaeff4ddb40), C(5d7cc5869baefef1), C(9644c5853af9cfeb),
		C(255c968184694ee1), C(4e4d726eda360927), C(7d27dd5b6d100377), C(9a300e2020ddea2c),
		C(5b5bedd)
	},
	{
		C(43c64d7484f7f9b2), C(5da002b64aafaeb7), C(b576c1e45800a716), C(3ee84d3d5b4ca00b), C(5cbc6d701894c3f9), C(d9e946f5ae1ca95), C(24ca06e67f0b1833),
		C(3ee84d3d5b4ca00b), C(5cbc6d701894c3f9), C(d9e946f5ae1ca95), C(24ca06e67f0b1833),
		C(3413d46b4152650e), C(cbdfdbc2ab516f9c), C(2aad8acb739e0c6c), C(2bfc950d9f9fa977),
		C(1aaddfa7)
	},
	{
		C(a7dec6ad81cf7fa1), C(180c1ab708683063), C(95e0fd7008d67cff), C(6b11c5073687208), C(7e0a57de0d453f3), C(e48c267d4f646867), C(2168e9136375f9cb),
		C(6b11c5073687208), C(7e0a57de0d453f3), C(e48c267d4f646867), C(2168e9136375f9cb),
		C(64da194aeeea7fdf), C(a3b9f01fa5885678), C(c316f8ee2eb2bd17), C(a7e4d80f83e4427f),
		C(5be07fd8)
	},
	{
		C(5408a1df99d4aff), C(b9565e588740f6bd), C(abf241813b08006e), C(7da9e81d89fda7ad), C(274157cabe71440d), C(2c22d9a480b331f7), C(e835c8ac746472d5),
		C(7da9e81d89fda7ad), C(274157cabe71440d), C(2c22d9a480b331f7), C(e835c8ac746472d5),
		C(2038ce817a201ae4), C(46f3289dfe1c5e40), C(435578a42d4b7c56), C(f96d9f409fcf561),
		C(cbca8606)
	},
	{
		C(a8b27a6bcaeeed4b), C(aec1eeded6a87e39), C(9daf246d6fed8326), C(d45a938b79f54e8f), C(366b219d6d133e48), C(5b14be3c25c49405), C(fdd791d48811a572),
		C(d45a938b79f54e8f), C(366b219d6d133e48), C(5b14be3c25c49405), C(fdd791d48811a572),
		C(3de67b8d9e95d335), C(903c01307cfbeed5), C(af7d65f32274f1d1), C(4dba141b5fc03c42),
		C(bde64d01)
	},
	{
		C(9a952a8246fdc269), C(d0dcfcac74ef278c), C(250f7139836f0f1f), C(c83d3c5f4e5f0320), C(694e7adeb2bf32e5), C(7ad09538a3da27f5), C(2b5c18f934aa5303),
		C(c83d3c5f4e5f0320), C(694e7adeb2bf32e5), C(7ad09538a3da27f5), C(2b5c18f934aa5303),
		C(c4dad7703d34326e), C(825569e2bcdc6a25), C(b83d267709ca900d), C(44ed05151f5d74e6),
		C(ee90cf33)
	},
	{
		C(c930841d1d88684f), C(5eb66eb18b7f9672), C(e455d413008a2546), C(bc271bc0df14d647), C(b071100a9ff2edbb), C(2b1a4c1cc31a119a), C(b5d7caa1bd946cef),
		C(bc271bc0df14d647), C(b071100a9ff2edbb), C(2b1a4c1cc31a119a), C(b5d7caa1bd946cef),
		C(e02623ae10f4aadd), C(d79f600389cd06fd), C(1e8da7965303e62b), C(86f50e10eeab0925),
		C(4305c3ce)
	},
	{
		C(94dc6971e3cf071a), C(994c7003b73b2b34), C(ea16e85978694e5), C(336c1b59a1fc19f6), C(c173acaecc471305), C(db1267d24f3f3f36), C(e9a5ee98627a6e78),
		C(336c1b59a1fc19f6), C(c173acaecc471305), C(db1267d24f3f3f36), C(e9a5ee98627a6e78),
		C(718f334204305ae5), C(e3b53c148f98d22c), C(a184012df848926), C(6e96386127d51183),
		C(4b3a1d76)
	},
	{
		C(7fc98006e25cac9), C(77fee0484cda86a7), C(376ec3d447060456), C(84064a6dcf916340), C(fbf55a26790e0ebb), C(2e7f84151c31a5c2), C(9f7f6d76b950f9bf),
		C(84064a6dcf916340), C(fbf55a26790e0ebb), C(2e7f84151c31a5c2), C(9f7f6d76b950f9bf),
		C(125e094fbee2b146), C(5706aa72b2eef7c2), C(1c4a2daa905ee66e), C(83d48029b5451694),
		C(a8bb6d80)
	},
	{
		C(bd781c4454103f6), C(612197322f49c931), C(b9cf17fd7e5462d5), C(e38e526cd3324364), C(85f2b63a5b5e840a), C(485d7cef5aaadd87), C(d2b837a462f6db6d),
		C(e38e526cd3324364), C(85f2b63a5b5e840a), C(485d7cef5aaadd87), C(d2b837a462f6db6d),
		C(3e41cef031520d9a), C(82df73902d7f67e), C(3ba6fd54c15257cb), C(22f91f079be42d40),
		C(1f9fa607)
	},
	{
		C(da60e6b14479f9df), C(3bdccf69ece16792), C(18ebf45c4fecfdc9), C(16818ee9d38c6664), C(5519fa9a1e35a329), C(cbd0001e4b08ed8), C(41a965e37a0c731b),
		C(16818ee9d38c6664), C(5519fa9a1e35a329), C(cbd0001e4b08ed8), C(41a965e37a0c731b),
		C(66e7b5dcca1ca28f), C(963b2d993614347d), C(9b6fc6f41d411106), C(aaaecaccf7848c0c),
		C(8d0e4ed2)
	},
	{
		C(4ca56a348b6c4d3), C(60618537c3872514), C(2fbb9f0e65871b09), C(30278016830ddd43), C(f046646d9012e074), C(c62a5804f6e7c9da), C(98d51f5830e2bc1e),
		C(30278016830ddd43), C(f046646d9012e074), C(c62a5804f6e7c9da), C(98d51f5830e2bc1e),
		C(7b2cbe5d37e3f29e), C(7b8c3ed50bda4aa0), C(3ea60cc24639e038), C(f7706de9fb0b5801),
		C(1bf31347)
	},
	{
		C(ebd22d4b70946401), C(6863602bf7139017), C(c0b1ac4e11b00666), C(7d2782b82bd494b6), C(97159ba1c26b304b), C(42b3b0fd431b2ac2), C(faa81f82691c830c),
		C(7d2782b82bd494b6), C(97159ba1c26b304b), C(42b3b0fd431b2ac2), C(faa81f82691c830c),
		C(7cc6449234c7e185), C(aeaa6fa643ca86a5), C(1412db1c0f2e0133), C(4df2fe3e4072934f),
		C(1ae3fc5b)
	},
	{
		C(3cc4693d6cbcb0c), C(501689ea1c70ffa), C(10a4353e9c89e364), C(58c8aba7475e2d95), C(3e2f291698c9427a), C(e8710d19c9de9e41), C(65dda22eb04cf953),
		C(58c8aba7475e2d95), C(3e2f291698c9427a), C(e8710d19c9de9e41), C(65dda22eb04cf953),
		C(d7729c48c250cffa), C(ef76162b2ddfba4b), C(52371e17f4d51f6d), C(ddd002112ff0c833),
		C(459c3930)
	},
	{
		C(38908e43f7ba5ef0), C(1ab035d4e7781e76), C(41d133e8c0a68ff7), C(d1090893afaab8bc), C(96c4fe6922772807), C(4522426c2b4205eb), C(efad99a1262e7e0d),
		C(d1090893afaab8bc), C(96c4fe6922772807), C(4522426c2b4205eb), C(efad99a1262e7e0d),
		C(c7696029abdb465e), C(4e18eaf03d517651), C(d006bced54c86ac8), C(4330326d1021860c),
		C(e00c4184)
	},
	{
		C(34983ccc6aa40205), C(21802cad34e72bc4), C(1943e8fb3c17bb8), C(fc947167f69c0da5), C(ae79cfdb91b6f6c1), C(7b251d04c26cbda3), C(128a33a79060d25e),
		C(fc947167f69c0da5), C(ae79cfdb91b6f6c1), C(7b251d04c26cbda3), C(128a33a79060d25e),
		C(1eca842dbfe018dd), C(50a4cd2ee0ba9c63), C(c2f5c97d8399682f), C(3f929fc7cbe8ecbb),
		C(ffc7a781)
	},
	{
		C(86215c45dcac9905), C(ea546afe851cae4b), C(d85b6457e489e374), C(b7609c8e70386d66), C(36e6ccc278d1636d), C(2f873307c08e6a1c), C(10f252a758505289),
		C(b7609c8e70386d66), C(36e6ccc278d1636d), C(2f873307c08e6a1c), C(10f252a758505289),
		C(c8977646e81ab4b6), C(8017b745cd80213b), C(960687db359bea0), C(ef4a470660799488),
		C(6a125480)
	},
	{
		C(420fc255c38db175), C(d503cd0f3c1208d1), C(d4684e74c825a0bc), C(4c10537443152f3d), C(720451d3c895e25d), C(aff60c4d11f513fd), C(881e8d6d2d5fb953),
		C(4c10537443152f3d), C(720451d3c895e25d), C(aff60c4d11f513fd), C(881e8d6d2d5fb953),
		C(9dec034a043f1f55), C(e27a0c22e7bfb39d), C(2220b959128324), C(53240272152dbd8b),
		C(88a1512b)
	},
	{
		C(1d7a31f5bc8fe2f9), C(4763991092dcf836), C(ed695f55b97416f4), C(f265edb0c1c411d7), C(30e1e9ec5262b7e6), C(c2c3ba061ce7957a), C(d975f93b89a16409),
		C(f265edb0c1c411d7), C(30e1e9ec5262b7e6), C(c2c3ba061ce7957a), C(d975f93b89a16409),
		C(e9d703123f43450a), C(41383fedfed67c82), C(6e9f43ecbbbd6004), C(c7ccd23a24e77b8),
		C(549bbbe5)
	},
	{
		C(94129a84c376a26e), C(c245e859dc231933), C(1b8f74fecf917453), C(e9369d2e9007e74b), C(b1375915d1136052), C(926c2021fe1d2351), C(1d943addaaa2e7e6),
		C(e9369d2e9007e74b), C(b1375915d1136052), C(926c2021fe1d2351), C(1d943addaaa2e7e6),
		C(f5f515869c246738), C(7e309cd0e1c0f2a0), C(153c3c36cf523e3b), C(4931c66872ea6758),
		C(c133d38c)
	},
	{
		C(1d3a9809dab05c8d), C(adddeb4f71c93e8), C(ef342eb36631edb), C(301d7a61c4b3dbca), C(861336c3f0552d61), C(12c6db947471300f), C(a679ef0ed761deb9),
		C(301d7a61c4b3dbca), C(861336c3f0552d61), C(12c6db947471300f), C(a679ef0ed761deb9),
		C(5f713b720efcd147), C(37ac330a333aa6b), C(3309dc9ec1616eef), C(52301d7a908026b5),
		C(fcace348)
	},
	{
		C(90fa3ccbd60848da), C(dfa6e0595b569e11), C(e585d067a1f5135d), C(6cef866ec295abea), C(c486c0d9214beb2d), C(d6e490944d5fe100), C(59df3175d72c9f38),
		C(6cef866ec295abea), C(c486c0d9214beb2d), C(d6e490944d5fe100), C(59df3175d72c9f38),
		C(3f23aeb4c04d1443), C(9bf0515cd8d24770), C(958554f60ccaade2), C(5182863c90132fe8),
		C(ed7b6f9a)
	},
	{
		C(2dbb4fc71b554514), C(9650e04b86be0f82), C(60f2304fba9274d3), C(fcfb9443e997cab), C(f13310d96dec2772), C(709cad2045251af2), C(afd0d30cc6376dad),
		C(fcfb9443e997cab), C(f13310d96dec2772), C(709cad2045251af2), C(afd0d30cc6376dad),
		C(59d4bed30d550d0d), C(58006d4e22d8aad1), C(eee12d2362d1f13b), C(35cf1d7faaf1d228),
		C(6d907dda)
	},
	{
		C(b98bf4274d18374a), C(1b669fd4c7f9a19a), C(b1f5972b88ba2b7a), C(73119c99e6d508be), C(5d4036a187735385), C(8fa66e192fd83831), C(2abf64b6b592ed57),
		C(73119c99e6d508be), C(5d4036a187735385), C(8fa66e192fd83831), C(2abf64b6b592ed57),
		C(d4501f95dd84b08c), C(bf1552439c8bea02), C(4f56fe753ba7e0ba), C(4ca8d35cc058cfcd),
		C(7a4d48d5)
	},
	{
		C(d6781d0b5e18eb68), C(b992913cae09b533), C(58f6021caaee3a40), C(aafcb77497b5a20b), C(411819e5e79b77a3), C(bd779579c51c77ce), C(58d11f5dcf5d075d),
		C(aafcb77497b5a20b), C(411819e5e79b77a3), C(bd779579c51c77ce), C(58d11f5dcf5d075d),
		C(9eae76cde1cb4233), C(32fe25a9bf657970), C(1c0c807948edb06a), C(b8f29a3dfaee254d),
		C(e686f3db)
	},
	{
		C(226651cf18f4884c), C(595052a874f0f51c), C(c9b75162b23bab42), C(3f44f873be4812ec), C(427662c1dbfaa7b2), C(a207ff9638fb6558), C(a738d919e45f550f),
		C(3f44f873be4812ec), C(427662c1dbfaa7b2), C(a207ff9638fb6558), C(a738d919e45f550f),
		C(cb186ea05717e7d6), C(1ca7d68a5871fdc1), C(5d4c119ea8ef3750), C(72b6a10fa2ff9406),
		C(cce7c55)
	},
	{
		C(a734fb047d3162d6), C(e523170d240ba3a5), C(125a6972809730e8), C(d396a297799c24a1), C(8fee992e3069bad5), C(2e3a01b0697ccf57), C(ee9c7390bd901cfa),
		C(d396a297799c24a1), C(8fee992e3069bad5), C(2e3a01b0697ccf57), C(ee9c7390bd901cfa),
		C(56f2d9da0af28af2), C(3fdd37b2fe8437cb), C(3d13eeeb60d6aec0), C(2432ae62e800a5ce),
		C(f58b96b)
	},
	{
		C(c6df6364a24f75a3), C(c294e2c84c4f5df8), C(a88df65c6a89313b), C(895fe8443183da74), C(c7f2f6f895a67334), C(a0d6b6a506691d31), C(24f51712b459a9f0),
		C(895fe8443183da74), C(c7f2f6f895a67334), C(a0d6b6a506691d31), C(24f51712b459a9f0),
		C(173a699481b9e088), C(1dee9b77bcbf45d3), C(32b98a646a8667d0), C(3adcd4ee28f42a0e),
		C(1bbf6f60)
	},
	{
		C(d8d1364c1fbcd10), C(2d7cc7f54832deaa), C(4e22c876a7c57625), C(a3d5d1137d30c4bd), C(1e7d706a49bdfb9e), C(c63282b20ad86db2), C(aec97fa07916bfd6),
		C(a3d5d1137d30c4bd), C(1e7d706a49bdfb9e), C(c63282b20ad86db2), C(aec97fa07916bfd6),
		C(7c9ba3e52d44f73e), C(af62fd245811185d), C(8a9d2dacd8737652), C(bd2cce277d5fbec0),
		C(ce5e0cc2)
	},
	{
		C(aae06f9146db885f), C(3598736441e280d9), C(fba339b117083e55), C(b22bf08d9f8aecf7), C(c182730de337b922), C(2b9adc87a0450a46), C(192c29a9cfc00aad),
		C(b22bf08d9f8aecf7), C(c182730de337b922), C(2b9adc87a0450a46), C(192c29a9cfc00aad),
		C(9fd733f1d84a59d9), C(d86bd5c9839ace15), C(af20b57303172876), C(9f63cb7161b5364c),
		C(584cfd6f)
	},
	{
		C(8955ef07631e3bcc), C(7d70965ea3926f83), C(39aed4134f8b2db6), C(882efc2561715a9c), C(ef8132a18a540221), C(b20a3c87a8c257c1), C(f541b8628fad6c23),
		C(882efc2561715a9c), C(ef8132a18a540221), C(b20a3c87a8c257c1), C(f541b8628fad6c23),
		C(9552aed57a6e0467), C(4d9fdd56867611a7), C(c330279bf23b9eab), C(44dbbaea2fcb8eba),
		C(8f9bbc33)
	},
	{
		C(ad611c609cfbe412), C(d3c00b18bf253877), C(90b2172e1f3d0bfd), C(371a98b2cb084883), C(33a2886ee9f00663), C(be9568818ed6e6bd), C(f244a0fa2673469a),
		C(371a98b2cb084883), C(33a2886ee9f00663), C(be9568818ed6e6bd), C(f244a0fa2673469a),
		C(b447050bd3e559e9), C(d3b695dae7a13383), C(ded0bb65be471188), C(ca3c7a2b78922cae),
		C(d7640d95)
	},
	{
		C(d5339adc295d5d69), C(b633cc1dcb8b586a), C(ee84184cf5b1aeaf), C(89f3aab99afbd636), C(f420e004f8148b9a), C(6818073faa797c7c), C(dd3b4e21cbbf42ca),
		C(89f3aab99afbd636), C(f420e004f8148b9a), C(6818073faa797c7c), C(dd3b4e21cbbf42ca),
		C(6a2b7db261164844), C(cbead63d1895852a), C(93d37e1eae05e2f9), C(5d06db2703fbc3ae),
		C(3d12a2b)
	},
	{
		C(40d0aeff521375a8), C(77ba1ad7ecebd506), C(547c6f1a7d9df427), C(21c2be098327f49b), C(7e035065ac7bbef5), C(6d7348e63023fb35), C(9d427dc1b67c3830),
		C(21c2be098327f49b), C(7e035065ac7bbef5), C(6d7348e63023fb35), C(9d427dc1b67c3830),
		C(4e3d018a43858341), C(cf924bb44d6b43c5), C(4618b6a26e3446ae), C(54d3013fac3ed469),
		C(aaeafed0)
	},
	{
		C(8b2d54ae1a3df769), C(11e7adaee3216679), C(3483781efc563e03), C(9d097dd3152ab107), C(51e21d24126e8563), C(cba56cac884a1354), C(39abb1b595f0a977),
		C(9d097dd3152ab107), C(51e21d24126e8563), C(cba56cac884a1354), C(39abb1b595f0a977),
		C(81e6dd1c1109848f), C(1644b209826d7b15), C(6ac67e4e4b4812f0), C(b3a9f5622c935bf7),
		C(95b9b814)
	},
	{
		C(99c175819b4eae28), C(932e8ff9f7a40043), C(ec78dcab07ca9f7c), C(c1a78b82ba815b74), C(458cbdfc82eb322a), C(17f4a192376ed8d7), C(6f9e92968bc8ccef),
		C(c1a78b82ba815b74), C(458cbdfc82eb322a), C(17f4a192376ed8d7), C(6f9e92968bc8ccef),
		C(93e098c333b39905), C(d59b1cace44b7fdc), C(f7a64ed78c64c7c5), C(7c6eca5dd87ec1ce),
		C(45fbe66e)
	},
	{
		C(2a418335779b82fc), C(af0295987849a76b), C(c12bc5ff0213f46e), C(5aeead8d6cb25bb9), C(739315f7743ec3ff), C(9ab48d27111d2dcc), C(5b87bd35a975929b),
		C(5aeead8d6cb25bb9), C(739315f7743ec3ff), C(9ab48d27111d2dcc), C(5b87bd35a975929b),
		C(c3dd8d6d95a46bb3), C(7bf9093215a4f483), C(cb557d6ed84285bd), C(daf58422f261fdb5),
		C(b4baa7a8)
	},
	{
		C(3b1fc6a3d279e67d), C(70ea1e49c226396), C(25505adcf104697c), C(ba1ffba29f0367aa), C(a20bec1dd15a8b6c), C(e9bf61d2dab0f774), C(f4f35bf5870a049c),
		C(ba1ffba29f0367aa), C(a20bec1dd15a8b6c), C(e9bf61d2dab0f774), C(f4f35bf5870a049c),
		C(26787efa5b92385), C(3d9533590ce30b59), C(a4da3e40530a01d4), C(6395deaefb70067c),
		C(83e962fe)
	},
	{
		C(d97eacdf10f1c3c9), C(b54f4654043a36e0), C(b128f6eb09d1234), C(d8ad7ec84a9c9aa2), C(e256cffed11f69e6), C(2cf65e4958ad5bda), C(cfbf9b03245989a7),
		C(d8ad7ec84a9c9aa2), C(e256cffed11f69e6), C(2cf65e4958ad5bda), C(cfbf9b03245989a7),
		C(9fa51e6686cf4444), C(9425c117a34609d5), C(b25f7e2c6f30e96), C(ea5477c3f2b5afd1),
		C(aac3531c)
	},
	{
		C(293a5c1c4e203cd4), C(6b3329f1c130cefe), C(f2e32f8ec76aac91), C(361e0a62c8187bff), C(6089971bb84d7133), C(93df7741588dd50b), C(c2a9b6abcd1d80b1),
		C(361e0a62c8187bff), C(6089971bb84d7133), C(93df7741588dd50b), C(c2a9b6abcd1d80b1),
		C(4d2f86869d79bc59), C(85cd24d8aa570ff), C(b0dcf6ef0e94bbb5), C(2037c69aa7a78421),
		C(2b1db7cc)
	},
	{
		C(4290e018ffaedde7), C(a14948545418eb5e), C(72d851b202284636), C(4ec02f3d2f2b23f2), C(ab3580708aa7c339), C(cdce066fbab3f65), C(d8ed3ecf3c7647b9),
		C(4ec02f3d2f2b23f2), C(ab3580708aa7c339), C(cdce066fbab3f65), C(d8ed3ecf3c7647b9),
		C(6d2204b3e31f344a), C(61a4d87f80ee61d7), C(446c43dbed4b728f), C(73130ac94f58747e),
		C(cf00cd31)
	},
	{
		C(f919a59cbde8bf2f), C(a56d04203b2dc5a5), C(38b06753ac871e48), C(c2c9fc637dbdfcfa), C(292ab8306d149d75), C(7f436b874b9ffc07), C(a5b56b0129218b80),
		C(c2c9fc637dbdfcfa), C(292ab8306d149d75), C(7f436b874b9ffc07), C(a5b56b0129218b80),
		C(9188f7bdc47ec050), C(cfe9345d03a15ade), C(40b520fb2750c49e), C(c2e83d343968af2e),
		C(7d3c43b8)
	},
	{
		C(1d70a3f5521d7fa4), C(fb97b3fdc5891965), C(299d49bbbe3535af), C(e1a8286a7d67946e), C(52bd956f047b298), C(cbd74332dd4204ac), C(12b5be7752721976),
		C(e1a8286a7d67946e), C(52bd956f047b298), C(cbd74332dd4204ac), C(12b5be7752721976),
		C(278426e27f6204b6), C(932ca7a7cd610181), C(41647321f0a5914d), C(48f4aa61a0ae80db),
		C(cbd5fac6)
	},
	{
		C(6af98d7b656d0d7c), C(d2e99ae96d6b5c0c), C(f63bd1603ef80627), C(bde51033ac0413f8), C(bc0272f691aec629), C(6204332651bebc44), C(1cbf00de026ea9bd),
		C(bde51033ac0413f8), C(bc0272f691aec629), C(6204332651bebc44), C(1cbf00de026ea9bd),
		C(b9c7ed6a75f3ff1e), C(7e310b76a5808e4f), C(acbbd1aad5531885), C(fc245f2473adeb9c),
		C(76d0fec4)
	},
	{
		C(395b7a8adb96ab75), C(582df7165b20f4a), C(e52bd30e9ff657f9), C(6c71064996cbec8b), C(352c535edeefcb89), C(ac7f0aba15cd5ecd), C(3aba1ca8353e5c60),
		C(6c71064996cbec8b), C(352c535edeefcb89), C(ac7f0aba15cd5ecd), C(3aba1ca8353e5c60),
		C(5c30a288a80ce646), C(c2940488b6617674), C(925f8cc66b370575), C(aa65d1283b9bb0ef),
		C(405e3402)
	},
	{
		C(3822dd82c7df012f), C(b9029b40bd9f122b), C(fd25b988468266c4), C(43e47bd5bab1e0ef), C(4a71f363421f282f), C(880b2f32a2b4e289), C(1299d4eda9d3eadf),
		C(43e47bd5bab1e0ef), C(4a71f363421f282f), C(880b2f32a2b4e289), C(1299d4eda9d3eadf),
		C(d713a40226f5564), C(4d8d34fedc769406), C(a85001b29cd9cac3), C(cae92352a41fd2b0),
		C(c732c481)
	},
	{
		C(79f7efe4a80b951a), C(dd3a3fddfc6c9c41), C(ab4c812f9e27aa40), C(832954ec9d0de333), C(94c390aa9bcb6b8a), C(f3b32afdc1f04f82), C(d229c3b72e4b9a74),
		C(832954ec9d0de333), C(94c390aa9bcb6b8a), C(f3b32afdc1f04f82), C(d229c3b72e4b9a74),
		C(1d11860d7ed624a6), C(cadee20b3441b984), C(75307079bf306f7b), C(87902aa3b9753ba4),
		C(a8d123c9)
	},
	{
		C(ae6e59f5f055921a), C(e9d9b7bf68e82), C(5ce4e4a5b269cc59), C(4960111789727567), C(149b8a37c7125ab6), C(78c7a13ab9749382), C(1c61131260ca151a),
		C(4960111789727567), C(149b8a37c7125ab6), C(78c7a13ab9749382), C(1c61131260ca151a),
		C(1e93276b35c309a0), C(2618f56230acde58), C(af61130a18e4febf), C(7145deb18e89befe),
		C(1e80ad7d)
	},
	{
		C(8959dbbf07387d36), C(b4658afce48ea35d), C(8f3f82437d8cb8d6), C(6566d74954986ba5), C(99d5235cc82519a7), C(257a23805c2d825), C(ad75ccb968e93403),
		C(6566d74954986ba5), C(99d5235cc82519a7), C(257a23805c2d825), C(ad75ccb968e93403),
		C(b45bd4cf78e11f7f), C(80c5536bdc487983), C(a4fd76ecbf018c8a), C(3b9dac78a7a70d43),
		C(52aeb863)
	},
	{
		C(4739613234278a49), C(99ea5bcd340bf663), C(258640912e712b12), C(c8a2827404991402), C(7ee5e78550f02675), C(2ec53952db5ac662), C(1526405a9df6794b),
		C(c8a2827404991402), C(7ee5e78550f02675), C(2ec53952db5ac662), C(1526405a9df6794b),
		C(eddc6271170c5e1f), C(f5a85f986001d9d6), C(95427c677bf58d58), C(53ed666dfa85cb29),
		C(ef7c0c18)
	},
	{
		C(420e6c926bc54841), C(96dbbf6f4e7c75cd), C(d8d40fa70c3c67bb), C(3edbc10e4bfee91b), C(f0d681304c28ef68), C(77ea602029aaaf9c), C(90f070bd24c8483c),
		C(3edbc10e4bfee91b), C(f0d681304c28ef68), C(77ea602029aaaf9c), C(90f070bd24c8483c),
		C(28bc8e41e08ceb86), C(1eb56e48a65691ef), C(9fea5301c9202f0e), C(3fcb65091aa9f135),
		C(b6ad4b68)
	},
	{
		C(c8601bab561bc1b7), C(72b26272a0ff869a), C(56fdfc986d6bc3c4), C(83707730cad725d4), C(c9ca88c3a779674a), C(e1c696fbbd9aa933), C(723f3baab1c17a45),
		C(83707730cad725d4), C(c9ca88c3a779674a), C(e1c696fbbd9aa933), C(723f3baab1c17a45),
		C(f82abc7a1d851682), C(30683836818e857d), C(78bfa3e89a5ab23f), C(6928234482b31817),
		C(c1e46b17)
	},
	{
		C(b2d294931a0e20eb), C(284ffd9a0815bc38), C(1f8a103aac9bbe6), C(1ef8e98e1ea57269), C(5971116272f45a8b), C(187ad68ce95d8eac), C(e94e93ee4e8ecaa6),
		C(1ef8e98e1ea57269), C(5971116272f45a8b), C(187ad68ce95d8eac), C(e94e93ee4e8ecaa6),
		C(a0ff2a58611838b5), C(b01e03849bfbae6f), C(d081e202e28ea3ab), C(51836bcee762bf13),
		C(57b8df25)
	},
	{
		C(7966f53c37b6c6d7), C(8e6abcfb3aa2b88f), C(7f2e5e0724e5f345), C(3eeb60c3f5f8143d), C(a25aec05c422a24f), C(b026b03ad3cca4db), C(e6e030028cc02a02),
		C(3eeb60c3f5f8143d), C(a25aec05c422a24f), C(b026b03ad3cca4db), C(e6e030028cc02a02),
		C(16fe679338b34bfc), C(c1be385b5c8a9de4), C(65af5df6567530eb), C(ed3b303df4dc6335),
		C(e9fa36d6)
	},
	{
		C(be9bb0abd03b7368), C(13bca93a3031be55), C(e864f4f52b55b472), C(36a8d13a2cbb0939), C(254ac73907413230), C(73520d1522315a70), C(8c9fdb5cf1e1a507),
		C(36a8d13a2cbb0939), C(254ac73907413230), C(73520d1522315a70), C(8c9fdb5cf1e1a507),
		C(b3640570b926886), C(fba2344ee87f7bab), C(de57341ab448df05), C(385612ee094fa977),
		C(8f8daefc)
	},
	{
		C(a08d128c5f1649be), C(a8166c3dbbe19aad), C(cb9f914f829ec62c), C(5b2b7ca856fad1c3), C(8093022d682e375d), C(ea5d163ba7ea231f), C(d6181d012c0de641),
		C(5b2b7ca856fad1c3), C(8093022d682e375d), C(ea5d163ba7ea231f), C(d6181d012c0de641),
		C(e7d40d0ab8b08159), C(2e82320f51b3a67e), C(27c2e356ea0b63a3), C(58842d01a2b1d077),
		C(6e1bb7e)
	},
	{
		C(7c386f0ffe0465ac), C(530419c9d843dbf3), C(7450e3a4f72b8d8c), C(48b218e3b721810d), C(d3757ac8609bc7fc), C(111ba02a88aefc8), C(e86343137d3bfc2a),
		C(48b218e3b721810d), C(d3757ac8609bc7fc), C(111ba02a88aefc8), C(e86343137d3bfc2a),
		C(44ad26b51661b507), C(db1268670274f51e), C(62a5e75beae875f3), C(e266e7a44c5f28c6),
		C(fd0076f0)
	},
	{
		C(bb362094e7ef4f8), C(ff3c2a48966f9725), C(55152803acd4a7fe), C(15747d8c505ffd00), C(438a15f391312cd6), C(e46ca62c26d821f5), C(be78d74c9f79cb44),
		C(15747d8c505ffd00), C(438a15f391312cd6), C(e46ca62c26d821f5), C(be78d74c9f79cb44),
		C(a8aa19f3aa59f09a), C(effb3cddab2c9267), C(d78e41ad97cb16a5), C(ace6821513527d32),
		C(899b17b6)
	},
	{
		C(cd80dea24321eea4), C(52b4fdc8130c2b15), C(f3ea100b154bfb82), C(d9ccef1d4be46988), C(5ede0c4e383a5e66), C(da69683716a54d1e), C(bfc3fdf02d242d24),
		C(d9ccef1d4be46988), C(5ede0c4e383a5e66), C(da69683716a54d1e), C(bfc3fdf02d242d24),
		C(20ed30274651b3f5), C(4c659824169e86c6), C(637226dae5b52a0e), C(7e050dbd1c71dc7f),
		C(e3e84e31)
	},
	{
		C(d599a04125372c3a), C(313136c56a56f363), C(1e993c3677625832), C(2870a99c76a587a4), C(99f74cc0b182dda4), C(8a5e895b2f0ca7b6), C(3d78882d5e0bb1dc),
		C(2870a99c76a587a4), C(99f74cc0b182dda4), C(8a5e895b2f0ca7b6), C(3d78882d5e0bb1dc),
		C(f466123732a3e25e), C(aca5e59716a40e50), C(261d2e7383d0e686), C(ce9362d6a42c15a7),
		C(eef79b6b)
	},
	{
		C(dbbf541e9dfda0a), C(1479fceb6db4f844), C(31ab576b59062534), C(a3335c417687cf3a), C(92ff114ac45cda75), C(c3b8a627384f13b5), C(c4f25de33de8b3f7),
		C(a3335c417687cf3a), C(92ff114ac45cda75), C(c3b8a627384f13b5), C(c4f25de33de8b3f7),
		C(eacbf520578c5964), C(4cb19c5ab24f3215), C(e7d8a6f67f0c6e7), C(325c2413eb770ada),
		C(868e3315)
	},
	{
		C(c2ee3288be4fe2bf), C(c65d2f5ddf32b92), C(af6ecdf121ba5485), C(c7cd48f7abf1fe59), C(ce600656ace6f53a), C(8a94a4381b108b34), C(f9d1276c64bf59fb),
		C(c7cd48f7abf1fe59), C(ce600656ace6f53a), C(8a94a4381b108b34), C(f9d1276c64bf59fb),
		C(219ce70ff5a112a5), C(e6026c576e2d28d7), C(b8e467f25015e3a6), C(950cb904f37af710),
		C(4639a426)
	},
	{
		C(d86603ced1ed4730), C(f9de718aaada7709), C(db8b9755194c6535), C(d803e1eead47604c), C(ad00f7611970a71b), C(bc50036b16ce71f5), C(afba96210a2ca7d6),
		C(d803e1eead47604c), C(ad00f7611970a71b), C(bc50036b16ce71f5), C(afba96210a2ca7d6),
		C(28f7a7be1d6765f0), C(97bd888b93938c68), C(6ad41d1b407ded49), C(b9bfec098dc543e4),
		C(f3213646)
	},
	{
		C(915263c671b28809), C(a815378e7ad762fd), C(abec6dc9b669f559), C(d17c928c5342477f), C(745130b795254ad5), C(8c5db926fe88f8ba), C(742a95c953e6d974),
		C(d17c928c5342477f), C(745130b795254ad5), C(8c5db926fe88f8ba), C(742a95c953e6d974),
		C(279db8057b5d3e96), C(98168411565b4ec4), C(50a72c54fa1125fa), C(27766a635db73638),
		C(17f148e9)
	},
	{
		C(2b67cdd38c307a5e), C(cb1d45bb5c9fe1c), C(800baf2a02ec18ad), C(6531c1fe32bcb417), C(8c970d8df8cdbeb4), C(917ba5fc67e72b40), C(4b65e4e263e0a426),
		C(6531c1fe32bcb417), C(8c970d8df8cdbeb4), C(917ba5fc67e72b40), C(4b65e4e263e0a426),
		C(e0de33ce88a8b3a9), C(f8ef98a437e16b08), C(a5162c0c7c5f7b62), C(dbdac43361b2b881),
		C(bfd94880)
	},
	{
		C(2d107419073b9cd0), C(a96db0740cef8f54), C(ec41ee91b3ecdc1b), C(ffe319654c8e7ebc), C(6a67b8f13ead5a72), C(6dd10a34f80d532f), C(6e9cfaece9fbca4),
		C(ffe319654c8e7ebc), C(6a67b8f13ead5a72), C(6dd10a34f80d532f), C(6e9cfaece9fbca4),
		C(b4468eb6a30aa7e9), C(e87995bee483222a), C(d036c2c90c609391), C(853306e82fa32247),
		C(bb1fa7f3)
	},
	{
		C(f3e9487ec0e26dfc), C(1ab1f63224e837fa), C(119983bb5a8125d8), C(8950cfcf4bdf622c), C(8847dca82efeef2f), C(646b75b026708169), C(21cab4b1687bd8b),
		C(8950cfcf4bdf622c), C(8847dca82efeef2f), C(646b75b026708169), C(21cab4b1687bd8b),
		C(243b489a9eae6231), C(5f3e634c4b779876), C(ff8abd1548eaf646), C(c7962f5f0151914b),
		C(88816b1)
	},
	{
		C(1160987c8fe86f7d), C(879e6db1481eb91b), C(d7dcb802bfe6885d), C(14453b5cc3d82396), C(4ef700c33ed278bc), C(1639c72ffc00d12e), C(fb140ee6155f700d),
		C(14453b5cc3d82396), C(4ef700c33ed278bc), C(1639c72ffc00d12e), C(fb140ee6155f700d),
		C(2e6b5c96a6620862), C(a1f136998cbe19c), C(74e058a3b6c5a712), C(93dcf6bd33928b17),
		C(5c2faeb3)
	},
	{
		C(eab8112c560b967b), C(97f550b58e89dbae), C(846ed506d304051f), C(276aa37744b5a028), C(8c10800ee90ea573), C(e6e57d2b33a1e0b7), C(91f83563cd3b9dda),
		C(276aa37744b5a028), C(8c10800ee90ea573), C(e6e57d2b33a1e0b7), C(91f83563cd3b9dda),
		C(afbb4739570738a1), C(440ba98da5d8f69), C(fde4e9b0eda20350), C(e67dfa5a2138fa1),
		C(51b5fc6f)
	},
	{
		C(1addcf0386d35351), C(b5f436561f8f1484), C(85d38e22181c9bb1), C(ff5c03f003c1fefe), C(e1098670afe7ff6), C(ea445030cf86de19), C(f155c68b5c2967f8),
		C(ff5c03f003c1fefe), C(e1098670afe7ff6), C(ea445030cf86de19), C(f155c68b5c2967f8),
		C(95d31b145dbb2e9e), C(914fe1ca3deb3265), C(6066020b1358ccc1), C(c74bb7e2dee15036),
		C(33d94752)
	},
	{
		C(d445ba84bf803e09), C(1216c2497038f804), C(2293216ea2237207), C(e2164451c651adfb), C(b2534e65477f9823), C(4d70691a69671e34), C(15be4963dbde8143),
		C(e2164451c651adfb), C(b2534e65477f9823), C(4d70691a69671e34), C(15be4963dbde8143),
		C(762e75c406c5e9a3), C(7b7579f7e0356841), C(480533eb066dfce5), C(90ae14ea6bfeb4ae),
		C(b0c92948)
	},
	{
		C(37235a096a8be435), C(d9b73130493589c2), C(3b1024f59378d3be), C(ad159f542d81f04e), C(49626a97a946096), C(d8d3998bf09fd304), C(d127a411eae69459),
		C(ad159f542d81f04e), C(49626a97a946096), C(d8d3998bf09fd304), C(d127a411eae69459),
		C(8f3253c4eb785a7b), C(4049062f37e62397), C(b9fa04d3b670e5c1), C(1211a7967ac9350f),
		C(c7171590)
	},
	{
		C(763ad6ea2fe1c99d), C(cf7af5368ac1e26b), C(4d5e451b3bb8d3d4), C(3712eb913d04e2f2), C(2f9500d319c84d89), C(4ac6eb21a8cf06f9), C(7d1917afcde42744),
		C(3712eb913d04e2f2), C(2f9500d319c84d89), C(4ac6eb21a8cf06f9), C(7d1917afcde42744),
		C(6b58604b5dd10903), C(c4288dfbc1e319fc), C(230f75ca96817c6e), C(8894cba3b763756c),
		C(240a67fb)
	},
	{
		C(ea627fc84cd1b857), C(85e372494520071f), C(69ec61800845780b), C(a3c1c5ca1b0367), C(eb6933997272bb3d), C(76a72cb62692a655), C(140bb5531edf756e),
		C(a3c1c5ca1b0367), C(eb6933997272bb3d), C(76a72cb62692a655), C(140bb5531edf756e),
		C(8d0d8067d1c925f4), C(7b3fa56d8d77a10c), C(2bd00287b0946d88), C(f08c8e4bd65b8970),
		C(e1843cd5)
	},
	{
		C(1f2ffd79f2cdc0c8), C(726a1bc31b337aaa), C(678b7f275ef96434), C(5aa82bfaa99d3978), C(c18f96cade5ce18d), C(38404491f9e34c03), C(891fb8926ba0418c),
		C(5aa82bfaa99d3978), C(c18f96cade5ce18d), C(38404491f9e34c03), C(891fb8926ba0418c),
		C(e5f69a6398114c15), C(7b8ded3623bc6b1d), C(2f3e5c5da5ff70e8), C(1ab142addea6a9ec),
		C(fda1452b)
	},
	{
		C(39a9e146ec4b3210), C(f63f75802a78b1ac), C(e2e22539c94741c3), C(8b305d532e61226e), C(caeae80da2ea2e), C(88a6289a76ac684e), C(8ce5b5f9df1cbd85),
		C(8b305d532e61226e), C(caeae80da2ea2e), C(88a6289a76ac684e), C(8ce5b5f9df1cbd85),
		C(8ae1fc4798e00d57), C(e7164b8fb364fc46), C(6a978c9bd3a66943), C(ef10d5ae4dd08dc),
		C(a2cad330)
	},
	{
		C(74cba303e2dd9d6d), C(692699b83289fad1), C(dfb9aa7874678480), C(751390a8a5c41bdc), C(6ee5fbf87605d34), C(6ca73f610f3a8f7c), C(e898b3c996570ad),
		C(751390a8a5c41bdc), C(6ee5fbf87605d34), C(6ca73f610f3a8f7c), C(e898b3c996570ad),
		C(98168a5858fc7110), C(6f987fa27aa0daa2), C(f25e3e180d4b36a3), C(d0b03495aeb1be8a),
		C(53467e16)
	},
	{
		C(4cbc2b73a43071e0), C(56c5db4c4ca4e0b7), C(1b275a162f46bd3d), C(b87a326e413604bf), C(d8f9a5fa214b03ab), C(8a8bb8265771cf88), C(a655319054f6e70f),
		C(b87a326e413604bf), C(d8f9a5fa214b03ab), C(8a8bb8265771cf88), C(a655319054f6e70f),
		C(b499cb8e65a9af44), C(bee7fafcc8307491), C(5d2e55fa9b27cda2), C(63b120f5fb2d6ee5),
		C(da14a8d0)
	},
	{
		C(875638b9715d2221), C(d9ba0615c0c58740), C(616d4be2dfe825aa), C(5df25f13ea7bc284), C(165edfaafd2598fb), C(af7215c5c718c696), C(e9f2f9ca655e769),
		C(5df25f13ea7bc284), C(165edfaafd2598fb), C(af7215c5c718c696), C(e9f2f9ca655e769),
		C(e459cfcb565d3d2d), C(41d032631be2418a), C(c505db05fd946f60), C(54990394a714f5de),
		C(67333551)
	},
	{
		C(fb686b2782994a8d), C(edee60693756bb48), C(e6bc3cae0ded2ef5), C(58eb4d03b2c3ddf5), C(6d2542995f9189f1), C(c0beec58a5f5fea2), C(ed67436f42e2a78b),
		C(58eb4d03b2c3ddf5), C(6d2542995f9189f1), C(c0beec58a5f5fea2), C(ed67436f42e2a78b),
		C(dfec763cdb2b5193), C(724a8d5345bd2d6), C(94d4fd1b81457c23), C(28e87c50cdede453),
		C(a0ebd66e)
	},
	{
		C(ab21d81a911e6723), C(4c31b07354852f59), C(835da384c9384744), C(7f759dddc6e8549a), C(616dd0ca022c8735), C(94717ad4bc15ceb3), C(f66c7be808ab36e),
		C(7f759dddc6e8549a), C(616dd0ca022c8735), C(94717ad4bc15ceb3), C(f66c7be808ab36e),
		C(af8286b550b2f4b7), C(745bd217d20a9f40), C(c73bfb9c5430f015), C(55e65922666e3fc2),
		C(4b769593)
	},
	{
		C(33d013cc0cd46ecf), C(3de726423aea122c), C(116af51117fe21a9), C(f271ba474edc562d), C(e6596e67f9dd3ebd), C(c0a288edf808f383), C(b3def70681c6babc),
		C(f271ba474edc562d), C(e6596e67f9dd3ebd), C(c0a288edf808f383), C(b3def70681c6babc),
		C(7da7864e9989b095), C(bf2f8718693cd8a1), C(264a9144166da776), C(61ad90676870beb6),
		C(6aa75624)
	},
	{
		C(8ca92c7cd39fae5d), C(317e620e1bf20f1), C(4f0b33bf2194b97f), C(45744afcf131dbee), C(97222392c2559350), C(498a19b280c6d6ed), C(83ac2c36acdb8d49),
		C(45744afcf131dbee), C(97222392c2559350), C(498a19b280c6d6ed), C(83ac2c36acdb8d49),
		C(7a69645c294daa62), C(abe9d2be8275b3d2), C(39542019de371085), C(7f4efac8488cd6ad),
		C(602a3f96)
	},
	{
		C(fdde3b03f018f43e), C(38f932946c78660), C(c84084ce946851ee), C(b6dd09ba7851c7af), C(570de4e1bb13b133), C(c4e784eb97211642), C(8285a7fcdcc7c58d),
		C(b6dd09ba7851c7af), C(570de4e1bb13b133), C(c4e784eb97211642), C(8285a7fcdcc7c58d),
		C(d421f47990da899b), C(8aed409c997eaa13), C(7a045929c2e29ccf), C(b373682a6202c86b),
		C(cd183c4d)
	},
	{
		C(9c8502050e9c9458), C(d6d2a1a69964beb9), C(1675766f480229b5), C(216e1d6c86cb524c), C(d01cf6fd4f4065c0), C(fffa4ec5b482ea0f), C(a0e20ee6a5404ac1),
		C(216e1d6c86cb524c), C(d01cf6fd4f4065c0), C(fffa4ec5b482ea0f), C(a0e20ee6a5404ac1),
		C(c1b037e4eebaf85e), C(634e3d7c3ebf89eb), C(bcda972358c67d1), C(fd1352181e5b8578),
		C(960a4d07)
	},
	{
		C(348176ca2fa2fdd2), C(3a89c514cc360c2d), C(9f90b8afb318d6d0), C(bceee07c11a9ac30), C(2e2d47dff8e77eb7), C(11a394cd7b6d614a), C(1d7c41d54e15cb4a),
		C(bceee07c11a9ac30), C(2e2d47dff8e77eb7), C(11a394cd7b6d614a), C(1d7c41d54e15cb4a),
		C(15baa5ae7312b0fc), C(f398f596cc984635), C(8ab8fdf87a6788e8), C(b2b5c1234ab47e2),
		C(9ae998c4)
	},
	{
		C(4a3d3dfbbaea130b), C(4e221c920f61ed01), C(553fd6cd1304531f), C(bd2b31b5608143fe), C(ab717a10f2554853), C(293857f04d194d22), C(d51be8fa86f254f0),
		C(bd2b31b5608143fe), C(ab717a10f2554853), C(293857f04d194d22), C(d51be8fa86f254f0),
		C(1eee39e07686907e), C(639039fe0e8d3052), C(d6ec1470cef97ff), C(370c82b860034f0f),
		C(74e2179d)
	},
	{
		C(b371f768cdf4edb9), C(bdef2ace6d2de0f0), C(e05b4100f7f1baec), C(b9e0d415b4ebd534), C(c97c2a27efaa33d7), C(591cdb35f84ef9da), C(a57d02d0e8e3756c),
		C(b9e0d415b4ebd534), C(c97c2a27efaa33d7), C(591cdb35f84ef9da), C(a57d02d0e8e3756c),
		C(23f55f12d7c5c87b), C(4c7ca0fe23221101), C(dbc3020480334564), C(d985992f32c236b1),
		C(ee9bae25)
	},
	{
		C(7a1d2e96934f61f), C(eb1760ae6af7d961), C(887eb0da063005df), C(2228d6725e31b8ab), C(9b98f7e4d0142e70), C(b6a8c2115b8e0fe7), C(b591e2f5ab9b94b1),
		C(2228d6725e31b8ab), C(9b98f7e4d0142e70), C(b6a8c2115b8e0fe7), C(b591e2f5ab9b94b1),
		C(6c1feaa8065318e0), C(4e7e2ca21c2e81fb), C(e9fe5d8ce7993c45), C(ee411fa2f12cf8df),
		C(b66edf10)
	},
	{
		C(8be53d466d4728f2), C(86a5ac8e0d416640), C(984aa464cdb5c8bb), C(87049e68f5d38e59), C(7d8ce44ec6bd7751), C(cc28d08ab414839c), C(6c8f0bd34fe843e3),
		C(87049e68f5d38e59), C(7d8ce44ec6bd7751), C(cc28d08ab414839c), C(6c8f0bd34fe843e3),
		C(b8496dcdc01f3e47), C(2f03125c282ac26), C(82a8797ba3f5ef07), C(7c977a4d10bf52b8),
		C(d6209737)
	},
	{
		C(829677eb03abf042), C(43cad004b6bc2c0), C(f2f224756803971a), C(98d0dbf796480187), C(fbcb5f3e1bef5742), C(5af2a0463bf6e921), C(ad9555bf0120b3a3),
		C(98d0dbf796480187), C(fbcb5f3e1bef5742), C(5af2a0463bf6e921), C(ad9555bf0120b3a3),
		C(283e39b3dc99f447), C(bedaa1a4a0250c28), C(9d50546624ff9a57), C(4abaf523d1c090f6),
		C(b994a88)
	},
	{
		C(754435bae3496fc), C(5707fc006f094dcf), C(8951c86ab19d8e40), C(57c5208e8f021a77), C(f7653fbb69cd9276), C(a484410af21d75cb), C(f19b6844b3d627e8),
		C(57c5208e8f021a77), C(f7653fbb69cd9276), C(a484410af21d75cb), C(f19b6844b3d627e8),
		C(f37400fc3ffd9514), C(36ae0d821734edfd), C(5f37820af1f1f306), C(be637d40e6a5ad0),
		C(a05d43c0)
	},
	{
		C(fda9877ea8e3805f), C(31e868b6ffd521b7), C(b08c90681fb6a0fd), C(68110a7f83f5d3ff), C(6d77e045901b85a8), C(84ef681113036d8b), C(3b9f8e3928f56160),
		C(68110a7f83f5d3ff), C(6d77e045901b85a8), C(84ef681113036d8b), C(3b9f8e3928f56160),
		C(fc8b7f56c130835), C(a11f3e800638e841), C(d9572267f5cf28c1), C(7897c8149803f2aa),
		C(c79f73a8)
	},
	{
		C(2e36f523ca8f5eb5), C(8b22932f89b27513), C(331cd6ecbfadc1bb), C(d1bfe4df12b04cbf), C(f58c17243fd63842), C(3a453cdba80a60af), C(5737b2ca7470ea95),
		C(d1bfe4df12b04cbf), C(f58c17243fd63842), C(3a453cdba80a60af), C(5737b2ca7470ea95),
		C(54d44a3f4477030c), C(8168e02d4869aa7f), C(77f383a17778559d), C(95e1737d77a268fc),
		C(a490aff5)
	},
	{
		C(21a378ef76828208), C(a5c13037fa841da2), C(506d22a53fbe9812), C(61c9c95d91017da5), C(16f7c83ba68f5279), C(9c0619b0808d05f7), C(83c117ce4e6b70a3),
		C(61c9c95d91017da5), C(16f7c83ba68f5279), C(9c0619b0808d05f7), C(83c117ce4e6b70a3),
		C(cfb4c8af7fd01413), C(fdef04e602e72296), C(ed6124d337889b1), C(4919c86707b830da),
		C(dfad65b4)
	},
	{
		C(ccdd5600054b16ca), C(f78846e84204cb7b), C(1f9faec82c24eac9), C(58634004c7b2d19a), C(24bb5f51ed3b9073), C(46409de018033d00), C(4a9805eed5ac802e),
		C(58634004c7b2d19a), C(24bb5f51ed3b9073), C(46409de018033d00), C(4a9805eed5ac802e),
		C(e18de8db306baf82), C(46bbf75f1fa025ff), C(5faf2fb09be09487), C(3fbc62bd4e558fb3),
		C(1d07dfb)
	},
	{
		C(7854468f4e0cabd0), C(3a3f6b4f098d0692), C(ae2423ec7799d30d), C(29c3529eb165eeba), C(443de3703b657c35), C(66acbce31ae1bc8d), C(1acc99effe1d547e),
		C(29c3529eb165eeba), C(443de3703b657c35), C(66acbce31ae1bc8d), C(1acc99effe1d547e),
		C(cf07f8a57906573d), C(31bafb0bbb9a86e7), C(40c69492702a9346), C(7df61fdaa0b858af),
		C(416df9a0)
	},
	{
		C(7f88db5346d8f997), C(88eac9aacc653798), C(68a4d0295f8eefa1), C(ae59ca86f4c3323d), C(25906c09906d5c4c), C(8dd2aa0c0a6584ae), C(232a7d96b38f40e9),
		C(ae59ca86f4c3323d), C(25906c09906d5c4c), C(8dd2aa0c0a6584ae), C(232a7d96b38f40e9),
		C(8986ee00a2ed0042), C(c49ae7e428c8a7d1), C(b7dd8280713ac9c2), C(e018720aed1ebc28),
		C(1f8fb9cc)
	},
	{
		C(bb3fb5fb01d60fcf), C(1b7cc0847a215eb6), C(1246c994437990a1), C(d4edc954c07cd8f3), C(224f47e7c00a30ab), C(d5ad7ad7f41ef0c6), C(59e089281d869fd7),
		C(d4edc954c07cd8f3), C(224f47e7c00a30ab), C(d5ad7ad7f41ef0c6), C(59e089281d869fd7),
		C(f29340d07a14b6f1), C(c87c5ef76d9c4ef3), C(463118794193a9a), C(2922dcb0540f0dbc),
		C(7abf48e3)
	},
	{
		C(2e783e1761acd84d), C(39158042bac975a0), C(1cd21c5a8071188d), C(b1b7ec44f9302176), C(5cb476450dc0c297), C(dc5ef652521ef6a2), C(3cc79a9e334e1f84),
		C(b1b7ec44f9302176), C(5cb476450dc0c297), C(dc5ef652521ef6a2), C(3cc79a9e334e1f84),
		C(769e2a283dbcc651), C(9f24b105c8511d3f), C(c31c15575de2f27e), C(ecfecf32c3ae2d66),
		C(dea4e3dd)
	},
	{
		C(392058251cf22acc), C(944ec4475ead4620), C(b330a10b5cb94166), C(54bc9bee7cbe1767), C(485820bdbe442431), C(54d6120ea2972e90), C(f437a0341f29b72a),
		C(54bc9bee7cbe1767), C(485820bdbe442431), C(54d6120ea2972e90), C(f437a0341f29b72a),
		C(8f30885c784d5704), C(aa95376b16c7906a), C(e826928cfaf93dc3), C(20e8f54d1c16d7d8),
		C(c6064f22)
	},
	{
		C(adf5c1e5d6419947), C(2a9747bc659d28aa), C(95c5b8cb1f5d62c), C(80973ea532b0f310), C(a471829aa9c17dd9), C(c2ff3479394804ab), C(6bf44f8606753636),
		C(80973ea532b0f310), C(a471829aa9c17dd9), C(c2ff3479394804ab), C(6bf44f8606753636),
		C(5184d2973e6dd827), C(121b96369a332d9a), C(5c25d3475ab69e50), C(26d2961d62884168),
		C(743bed9c)
	},
	{
		C(6bc1db2c2bee5aba), C(e63b0ed635307398), C(7b2eca111f30dbbc), C(230d2b3e47f09830), C(ec8624a821c1caf4), C(ea6ec411cdbf1cb1), C(5f38ae82af364e27),
		C(230d2b3e47f09830), C(ec8624a821c1caf4), C(ea6ec411cdbf1cb1), C(5f38ae82af364e27),
		C(a519ef515ea7187c), C(6bad5efa7ebae05f), C(748abacb11a74a63), C(a28eef963d1396eb),
		C(fce254d5)
	},
	{
		C(b00f898229efa508), C(83b7590ad7f6985c), C(2780e70a0592e41d), C(7122413bdbc94035), C(e7f90fae33bf7763), C(4b6bd0fb30b12387), C(557359c0c44f48ca),
		C(7122413bdbc94035), C(e7f90fae33bf7763), C(4b6bd0fb30b12387), C(557359c0c44f48ca),
		C(d5656c3d6bc5f0d), C(983ff8e5e784da99), C(628479671b445bf), C(e179a1e27ce68f5d),
		C(e47ec9d1)
	},
	{
		C(b56eb769ce0d9a8c), C(ce196117bfbcaf04), C(b26c3c3797d66165), C(5ed12338f630ab76), C(fab19fcb319116d), C(167f5f42b521724b), C(c4aa56c409568d74),
		C(5ed12338f630ab76), C(fab19fcb319116d), C(167f5f42b521724b), C(c4aa56c409568d74),
		C(75fff4b42f8e9778), C(94218f94710c1ea3), C(b7b05efb738b06a6), C(83fff2deabf9cd3),
		C(334a145c)
	},
	{
		C(70c0637675b94150), C(259e1669305b0a15), C(46e1dd9fd387a58d), C(fca4e5bc9292788e), C(cd509dc1facce41c), C(bbba575a59d82fe), C(4e2e71c15b45d4d3),
		C(fca4e5bc9292788e), C(cd509dc1facce41c), C(bbba575a59d82fe), C(4e2e71c15b45d4d3),
		C(5dc54582ead999c), C(72612d1571963c6f), C(30318a9d2d3d1829), C(785dd00f4cc9c9a0),
		C(adec1e3c)
	},
	{
		C(74c0b8a6821faafe), C(abac39d7491370e7), C(faf0b2a48a4e6aed), C(967e970df9673d2a), C(d465247cffa415c0), C(33a1df0ca1107722), C(49fc2a10adce4a32),
		C(967e970df9673d2a), C(d465247cffa415c0), C(33a1df0ca1107722), C(49fc2a10adce4a32),
		C(c5707e079a284308), C(573028266635dda6), C(f786f5eee6127fa0), C(b30d79cebfb51266),
		C(f6a9fbf8)
	},
	{
		C(5fb5e48ac7b7fa4f), C(a96170f08f5acbc7), C(bbf5c63d4f52a1e5), C(6cc09e60700563e9), C(d18f23221e964791), C(ffc23eeef7af26eb), C(693a954a3622a315),
		C(815308a32a9b0daf), C(efb2ab27bf6fd0bd), C(9f1ffc0986111118), C(f9a3aa1778ea3985),
		C(698fe54b2b93933b), C(dacc2b28404d0f10), C(815308a32a9b0daf), C(efb2ab27bf6fd0bd),
		C(5398210c)
	},
};

void Check(uint64 expected, uint64 actual)
{
	if (expected != actual)
	{
		cerr << "ERROR: expected 0x" << hex << expected << ", but got 0x" << actual << "\n";
		++errors;
	}
}

void Test(const uint64 *expected, int offset, int len)
{
	const uint128 u = CityHash128(data + offset, len);
	const uint128 v = CityHash128WithSeed(data + offset, len, kSeed128);
	Check(expected[0], CityHash64(data + offset, len));
	Check(expected[15], CityHash32(data + offset, len));
	Check(expected[1], CityHash64WithSeed(data + offset, len, kSeed0));
	Check(expected[2], CityHash64WithSeeds(data + offset, len, kSeed0, kSeed1));
	Check(expected[3], Uint128Low64(u));
	Check(expected[4], Uint128High64(u));
	Check(expected[5], Uint128Low64(v));
	Check(expected[6], Uint128High64(v));
#ifdef __SSE4_2__
	const uint128 y = CityHashCrc128(data + offset, len);
	const uint128 z = CityHashCrc128WithSeed(data + offset, len, kSeed128);
	uint64 crc256_results[4];
	CityHashCrc256(data + offset, len, crc256_results);
	Check(expected[7], Uint128Low64(y));
	Check(expected[8], Uint128High64(y));
	Check(expected[9], Uint128Low64(z));
	Check(expected[10], Uint128High64(z));
	for (int i = 0; i < 4; i++)
	{
		Check(expected[11 + i], crc256_results[i]);
	}
#endif
}

#else

#define Test(a, b, c) Dump((b), (c))
void Dump(int offset, int len)
{
	const uint128 u = CityHash128(data + offset, len);
	const uint128 v = CityHash128WithSeed(data + offset, len, kSeed128);
	const uint128 y = CityHashCrc128(data + offset, len);
	const uint128 z = CityHashCrc128WithSeed(data + offset, len, kSeed128);
	uint64 crc256_results[4];
	CityHashCrc256(data + offset, len, crc256_results);
	cout << hex
	     << "{C(" << CityHash64(data + offset, len) << "), "
	     << "C(" << CityHash64WithSeed(data + offset, len, kSeed0) << "), "
	     << "C(" << CityHash64WithSeeds(data + offset, len, kSeed0, kSeed1) << "), "
	     << "C(" << Uint128Low64(u) << "), "
	     << "C(" << Uint128High64(u) << "), "
	     << "C(" << Uint128Low64(v) << "), "
	     << "C(" << Uint128High64(v) << "),\n"
	     << "C(" << Uint128Low64(y) << "), "
	     << "C(" << Uint128High64(y) << "), "
	     << "C(" << Uint128Low64(z) << "), "
	     << "C(" << Uint128High64(z) << "),\n";
	for (int i = 0; i < 4; i++)
	{
		cout << hex << "C(" << crc256_results[i] << (i == 3 ? "),\n" : "), ");
	}
	cout << "C(" << CityHash32(data + offset, len) << ")},\n";
}

#endif

int main(int argc, char **argv)
{
	setup();
	int i = 0;
	for ( ; i < kTestSize - 1; i++)
	{
		Test(testdata[i], i * i, i);
	}
	Test(testdata[i], 0, kDataSize);
	return errors > 0;
}
