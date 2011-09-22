%module hyperclient
%include "std_map.i"
%include "std_pair.i"
%include "std_string.i"
%include "std_vector.i"
%include "stdint.i"
%include "various.i"
%{
#include "hyperclient/client.h"
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

class client
{
    public:
        client(po6::net::location coordinator);
        ~client() throw ();

    public:
        int connect();

    public:
        int get(const std::string& space, const e::buffer& key,
                std::map<std::string, e::buffer>* value);
        int put(const std::string& space, const e::buffer& key,
                const std::map<std::string, e::buffer>& value);
        int del(const std::string& space, const e::buffer& key);
        int search(const std::string& space,
                   const std::map<std::string, e::buffer>& equality,
                   const std::map<std::string, std::pair<uint64_t, uint64_t> >& range,
                   std::vector<std::map<std::string, e::buffer> >* results);

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
        buffer(const char* BYTE, int LENGTH);

    public:
        std::string str() const;
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
    %template(rangepair) std::pair<uint64_t, uint64_t>;
    %template(rangesearch) map<string, std::pair<uint64_t, uint64_t> >;
    %template(searchresult) std::vector<map<string, e::buffer> >;
}
