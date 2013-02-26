# Copyright (c) 2012, Cornell University
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

'''
This test is intended primarily as a demonstration of how to write HyperUnit
tests.  The test is isolated into a single function, named "test", and invoked
with a config by a simple "am I run as a script" wrapper.  This ensures the
test can be run along with other tests without starting up and tearing down
a hyperdex environment.

For information on how to write the tests, refer to the actual implementation
of test.
'''

def test_phonebook(cfg):
	'creates phonebook, puts jdoe, and gets it back'
	cfg.require_daemons(5)

	# must() sets up a test context that will raise exceptions on failure;
	# we don't want to continue the test if we fail to create our space.
	cfg.must().add_space(str('''
		space phonebook
		key username
		attributes first, last, int phone
		subspace first, last, phone
		create 8 partitions
		tolerate 2 failures
	'''))

	# we plan on inserting these documents into HyperDex and verifying them
	jdoe = {'first' : 'john', 'last' : 'doe', 'phone': 5551234567}
	edoe = {'first' : 'eve', 'last' : 'doe', 'phone': 5551234568}

	# we "must" put tehese documents in, otherwise we can't verify them later.
	cfg.must().put('phonebook', 'jdoe', jdoe)
	cfg.must().put('phonebook', 'edoe', edoe)

	# by using 'should', here, we state that we should be able to
	# find the record, and when we do, it should be equivalent.
	cfg.should().get('phonebook','jdoe').equal(jdoe)

	# we can chain 'shouldnt' here to assert that while we should
	# be able to find this record, it shouldn't be equivalent.
	cfg.should().get('phonebook','jdoe').shouldnt().equals(edoe)

	# we 'shouldnt' be able to find a nonexistent key.
	cfg.should().get('phonebook', 'nonesuch').equals(None)

	# it is good ettiquite for a test to clear out its space
	cfg.must().rm_space('phonebook')

if __name__ == '__main__':
	import hyperunit

	with hyperunit.Config() as cfg:
		test_phonebook(cfg)
