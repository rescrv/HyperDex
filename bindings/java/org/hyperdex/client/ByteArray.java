package org.hyperdex.client;

import java.util.*;
import java.io.UnsupportedEncodingException;

public class ByteArray implements Comparable
{
    private byte[] bytes = null; 

    private String defaultEncoding = "UTF-8";

    public ByteArray(byte[] bytes)
    {
        this.bytes = bytes;   
    }

    public ByteArray(String str, String encoding) throws TypeError
    {
        try
        {
            this.bytes = str.getBytes(encoding);
        }
        catch(UnsupportedEncodingException usee)
        {
            throw new TypeError("Could not encode java string '"
                                    + str + "' into bytes using encoding '"
                                    + encoding +"'"); 
        }
    }

    public static ByteArray wrap(byte[] bytes)
    {
        return new ByteArray(bytes);
    }

    public static ByteArray encode(String str, String encoding) throws TypeError
    {
        try
        {
            return new ByteArray(str.getBytes(encoding));
        }
        catch(UnsupportedEncodingException usee)
        {
            throw new TypeError("Could not encode java string '"
                                    + str + "' into bytes using encoding '"
                                    + encoding +"'"); 
        }
    }

    public String getDefaultEncoding()
    {
        return defaultEncoding;
    }

    public void setDefaultEncoding(String encoding)
    {
        defaultEncoding = encoding;
    }

    public String decode(String encoding) throws TypeError
    {
        try
        {
            return new String(bytes,encoding);
        }
        catch(UnsupportedEncodingException usee)
        {
            throw new TypeError("Could not decode bytes using encoding '"
                                    + encoding +"'"); 
        }
    }

    public static String decode(byte[] bytes, String encoding) throws TypeError
    {
        try
        {
            return new String(bytes, encoding);
        }
        catch(UnsupportedEncodingException usee)
        {
            throw new TypeError("Could not decode bytes using encoding '"
                                    + encoding +"'"); 
        }
    }

    public static String decode(ByteArray ba, String encoding) throws TypeError
    {
        return decode(ba.getBytes(), encoding);
    }

    public String toString(String encoding) throws TypeError
    {
        return decode(encoding);
    }

    public static String toString(byte[] bytes, String encoding) throws TypeError
    {
        return  decode(bytes, encoding);
    }

    public String toString()
    {
        String ret = "";

        try
        {
            ret = toString(defaultEncoding);
        }
        catch(TypeError e)
        {
        }

        return ret;
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    private static int compare(byte[] b1, byte[] b2)
    {
        int minSize = ( b1.length < b2.length )?b1.length:b2.length;

        for (int i=0; i<minSize; i++)
        {
            if ( (b1[i] & 0x000000ff) < (b2[i] & 0x000000ff) )
            {
                return -1;
            }
            else if ( (b1[i] & 0x000000ff) > (b2[i] & 0x000000ff) )
            {
                return 1;
            }
        }

        if ( b1.length < b2.length )
        {
            return -1;
        }
        else if ( b1.length > b2.length )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    public int compareTo(Object o)
    {
        return compare(bytes,((ByteArray)o).getBytes());
    }

    public boolean equals(Object o)
    {
        return Arrays.equals(bytes,((ByteArray)o).getBytes());
    }

    public int hashCode()
    {
        return Arrays.hashCode(bytes);
    }
}
