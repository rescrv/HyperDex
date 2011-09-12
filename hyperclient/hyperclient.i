%module hyperclient
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%{
#include "hyperclient/client.h"
#include "hyperclient/result.h"
%}
%include "enums.swg"
%javaconst(1);

%pragma(java) jniclasscode=
%{
    static
    {
        try
        {
            System.loadLibrary("hyperclient-java");
        }
        catch (UnsatisfiedLinkError e)
        {
            System.err.println("Could not load libhyperclient-java.so:\n" + e);
            System.exit(1);
        }
    }
%}

namespace hyperclient
{

class result
{
    public:
        result();
        ~result() throw ();

    public:
        int status();
};

class get_result
{
    public:
        get_result();
        ~get_result() throw ();

    public:
        int status();
        std::vector<e::buffer>& value();
};

class client
{
    public:
        client(po6::net::location coordinator);
        ~client() throw ();

    public:
        int connect();

    public:
        void get(const std::string& space, const e::buffer& key, get_result& r);
        void put(const std::string& space, const e::buffer& key,
                 const std::vector<e::buffer>& value, result& r);
        void del(const std::string& space, const e::buffer& key, result& r);
        void update(const std::string& space, const e::buffer& key,
                    const std::map<std::string, e::buffer>& value, result& r);

    public:
        size_t outstanding();
        int flush(int timeout);
        int flush_one(int timeout);
};

}

namespace e
{

class buffer
{
    public:
        buffer(const std::string& buf);
};

}

namespace po6
{
namespace net
{

class location
{
    public:
        location(const char* _address, int _port);
};

}
}

namespace std
{
    %template(vectorbuffer) vector<e::buffer>;
    %template(mapstrbuf) map<string, e::buffer>;
}
