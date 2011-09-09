%module hyperclient
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%{
#include "hyperclient/sync_client.h"
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

class sync_client
{
    public:
        sync_client(po6::net::location coordinator);

    public:
        int connect();

    public:
        int get(const std::string& space, const e::buffer& key, std::vector<e::buffer>* value);
        int put(const std::string& space, const e::buffer& key, const std::vector<e::buffer>& value);
        int del(const std::string& space, const e::buffer& key);
        int update(const std::string& space, const e::buffer& key, const std::map<std::string, e::buffer>& value);

    private:
        struct priv;
        const std::auto_ptr<priv> p;
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
