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

int main(int c, char *v[])
{
	const char *host = "127.0.0.1";
	uint16_t port = 1982;
	try
	{
		Admin h(host, port);
		hyperdex_admin_returncode rrc;
		const char *spaces;
		int64_t rid = h.list_spaces(&rrc, &spaces);
		if (rid < 0)
		{
			std::cerr << "could not list spaces: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		hyperdex_admin_returncode lrc;
		int64_t lid = h.loop(-1, &lrc);
		if (lid < 0)
		{
			std::cerr << "could not list spaces: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		assert(rid == lid);
		if (rrc != HYPERDEX_ADMIN_SUCCESS)
		{
			std::cerr << "could not list spaces: " << h.error_message() << std::endl;
			return EXIT_FAILURE;
		}
		if (strcmp(spaces, "") == 0)
		{
			std::cout << "No spaces are found." << std::endl;
		}
		else
		{
			std::cout << spaces;
			return EXIT_SUCCESS;
		}
	}
	catch (std::exception &e)
	{
		std::cerr << "error: " << e.what() << std::endl;
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
