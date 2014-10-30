def assertEqualsApprox(actual, expected, tolerance):
	if not (abs(actual - expected) < tolerance):
		print "AssertEqualsApprox failed"
		print "Should be: " + str(expected) + ", but was " + str(actual) + "."

		assert False

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
