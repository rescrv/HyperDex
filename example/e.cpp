#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <assert.h>
#include <hyperdex/admin.hpp>
#include <hyperdex/client.hpp>

using namespace std;
using namespace hyperdex;

static const char rcsid[] = "$Id: " __FILE__ ",v 1.0.0-0 " __DATE__ " " __TIME__ " Cnangel Exp $";

int
main(int argc, const char *argv[])
{
	char *p = (char *)malloc(1000);
	if (p == NULL)
	{
		return 0;
	}
	memset(p, 'A', 1000);
	printf("1 %p\n", p);
	p = (char *)realloc(p, 200);
	if (p == NULL)
	{
		return 0;
	}
	memset(p, 'B', 200);
	printf("2 %p\n", p);
	free(p);
	p = new char[1000];
	if (p == NULL)
	{
		return 0;
	}
	memset(p, 'A', 1000);
	printf("3 %p\n", p);
	p = new char[200];
	if (p == NULL)
	{
		return 0;
	}
	memset(p, 'B', 200);
	printf("4 %p\n", p);
	free(p);
	return 0;
}
