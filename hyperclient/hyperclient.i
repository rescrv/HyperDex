%module hyperclient
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
        status get(const std::string& space, const e::buffer& key, std::vector<e::buffer>* value);
        status put(const std::string& space, const e::buffer& key, const std::vector<e::buffer>& value);
        status del(const std::string& space, const e::buffer& key);
        status update(const std::string& space, const e::buffer& key, const std::map<std::string, e::buffer>& value);

    private:
        struct priv;
        const std::auto_ptr<priv> p;
};

}
