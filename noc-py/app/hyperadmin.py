import hyperdex.admin

a = hyperdex.admin.Admin('127.0.0.1', 1982)

def delspace(space):
	a.rm_space(space.encode('utf-8'))

def create_space(newspace, attrlist):
	spacename = newspace[0]
	keyname = newspace[1]
	partitions = newspace[2]
	failures = newspace[3]

	attributes = ''
	for attribute in attrlist[:-1]:
		attributes = attributes + attribute[0] + ' ' + attribute[1] + ', '
	lastattr = attrlist[-1]
	attributes = attributes + lastattr[0] + ' ' + lastattr[1]
	subslist = []
	for attribute in attrlist:
		if attribute[2] is True:
			subslist.append(attribute)
	subspaces = ''
	for attribute in subslist[:-1]:
		subspaces = subspaces + subslist[1] + ', '
	lastsub = subslist[-1]
	subspaces = subspaces + lastsub[1]

	a.add_space('''
		space %s
		key %s
		attributes
			%s
		subspace %s
		create %d partitions
		tolerate %d failures
		''' % (spacename, keyname, attributes, subspaces, partitions, failures))
	
