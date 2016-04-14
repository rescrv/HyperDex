#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <assert.h>

using namespace std;

static const char rcsid[] = "$Id: " __FILE__ ",v 1.0.0-0 " __DATE__ " " __TIME__ " Cnangel Exp $";
#define BUFFER_SIZE  1024*1024*100

int
main(int argc, const char *argv[])
{
	const char *file = "./data.txt";
	if (argc == 1)
	{
		char *p = (char *)malloc(BUFFER_SIZE);
		memset(p, 'z', BUFFER_SIZE);
		fstream fs;
		fs.open(file, std::ios::out | std::ios::trunc);
		fs << p;
		fs.close();
		free(p);
	}
	else if (argc == 2)
	{
		char *pp = (char *)malloc(BUFFER_SIZE);
		fstream fs;
		fs.open(file, ios::in | ios::binary);
		fs.getline(pp, BUFFER_SIZE);
		fs.close();
		printf("pp: %.*s", 100, pp);
		free(pp);
	}
	else
	{
		unlink(file);
	}
}
