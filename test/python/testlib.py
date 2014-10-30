def assertEqualsApprox(actual, expected, tolerance):
    # Recurse over all subdocuments
    if isinstance(actual, dict) and isinstance(expected, dict):
        for k,v in actual.iteritems():
            assert k in expected
            assertEqualsApprox(v, expected[k], tolerance)
        
    elif isinstance(actual, float) and isinstance(expected, float):
        if not (abs(actual - expected) < tolerance):
            print "AssertEqualsApprox failed"
            print "Should be: " + str(expected) + ", but was " + str(actual) + "."

            assert False

    else:
        raise ValueError('Invalid Type(s) in AssertEqualsApprox')


def assertEquals(actual, expected):
	if not actual == expected:
		print "AssertEquals failed"
		print "Should be: " + str(expected) + ", but was " + str(actual) + "."

		assert False

def assertTrue(val):
	if not val:
		print "AssertTrue failed"
		assert False

def assertFalse(val):
	if val:
		print "AssertFalse failed"
		assert False
