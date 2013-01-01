package hyperclient;

import java.util.*;
import java.io.UnsupportedEncodingException;

public class ByteArrayKeyedMap<V> extends HashMap<ByteArray,V>
{
    private String defaultEncoding = "UTF-8";

    public ByteArrayKeyedMap()
    {
        super();
    }

    public ByteArrayKeyedMap(String defaultEncoding)
    {
        super();
        this.defaultEncoding = defaultEncoding;
    }

    private Object getByteArrayObjectFromObject(Object o)
    {
        
        try
        {
		    if ( o instanceof byte[] )
		    {
		        return new ByteArray((byte[])o);
		    }
		    else if ( o instanceof ByteArray )
		    {
		        return (ByteArray)o;
		    }
		    else if ( o instanceof String )
		    {
		        return new ByteArray((String)o,defaultEncoding);
		    }
	        else
	        {
	            return o;
	        }
        }
        catch(TypeError e)
        {
            return o;
        }
    }

    public boolean containsKey(Object key)
    {
        return super.containsKey(getByteArrayObjectFromObject(key));
    }

    public V get(Object key)
    {
        return super.get(getByteArrayObjectFromObject(key));
    }

    public void put(byte[] keyBytes, V val)
    {
        super.put(new ByteArray(keyBytes),val);
    }

    public void put(String keyStr, V val) throws TypeError
    {
        super.put(new ByteArray(keyStr,defaultEncoding),val);
    }

    public V remove(Object key)
    {
        return super.remove(getByteArrayObjectFromObject(key));
    }
}
