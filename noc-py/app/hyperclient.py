import hyperdex.client


def getall(space):
	c = hyperdex.client.Client('127.0.0.1',1982)
	print space
	situation = [x for x in c.search(space, {})]
	print situation
	return situation