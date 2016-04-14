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
	//std::string description;
	const char *host = "127.0.0.1";
	uint16_t port = 1982;
	stringstream ss;
	ss << "space phonebook1 key username attributes first, last, int phone subspace first, last, phone create 8 partitions tolerate 2 failures";
	try
	{
		Admin h(host, port);
		hyperdex_admin_returncode rrc;
		//const char *description = ss.str().c_str();
		string sa = ss.str();
		const char *description = sa.c_str();
		cout << description << endl;
		int64_t rid = h.add_space(description, &rrc);
		if (rid < 0)
		{
			std::cerr << "could not add space1: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		hyperdex_admin_returncode lrc;
		cout << "-------------------" << endl;
		int64_t lid = h.loop(-1, &lrc);
		if (lid < 0)
		{
			std::cerr << "could not add space2: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		assert(rid == lid);
		if (rrc != HYPERDEX_ADMIN_SUCCESS)
		{
			std::cerr << "could not add space3: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		return EXIT_SUCCESS;
	}
	catch (std::exception &e)
	{
		std::cerr << "error: " << e.what() << std::endl;
		return EXIT_FAILURE;
	}
}
