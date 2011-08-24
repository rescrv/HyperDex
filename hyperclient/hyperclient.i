%module hyperclient
%include "std_map.i"
%include "std_string.i"
%include "std_vector.i"
%{
#include "hyperclient/client.h"
%}
%include "enums.swg"
%javaconst(1);

namespace hyperclient
{

enum status
{
    SUCCESS     = 0,
    NOTFOUND    = 1,
    WRONGARITY  = 2,
    NOTASPACE   = 8,
    BADSEARCH   = 9,
    BADDIMENSION= 10,
    COORDFAIL   = 16,
    SERVERERROR = 17,
    CONNECTFAIL = 18,
    DISCONNECT  = 19,
    RECONFIGURE = 20,
    LOGICERROR  = 21
};

class client
{
    public:
        client(po6::net::location coordinator);

    public:
        status connect();

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
