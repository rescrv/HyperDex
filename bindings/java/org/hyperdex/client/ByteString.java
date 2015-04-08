/* Copyright (c) 2011-2013, Nick Tolomiczenko
 * Copyright (c) 2013-2015, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.hyperdex.client;

import java.util.*;
import java.io.UnsupportedEncodingException;

public class ByteString
{
    private byte[] bytes = null;
    private String defaultEncoding = "UTF-8";

    public ByteString(byte[] bytes)
    {
        this.bytes = bytes;
    }

    public ByteString(String str, String encoding)
        throws UnsupportedEncodingException
    {
        this.bytes = str.getBytes(encoding);
    }
    
    public ByteString(String str)
    {
        try {
            this.bytes = str.getBytes(defaultEncoding);
        } catch(UnsupportedEncodingException e) {
            // This should never happen. 
            throw new RuntimeException(e.getMessage()); 
        }
    }

    public static ByteString wrap(byte[] bytes)
    {
        return new ByteString(bytes);
    }

    public static ByteString encode(String str, String encoding)
        throws UnsupportedEncodingException
    {
        return new ByteString(str.getBytes(encoding));
    }

    public static String decode(ByteString bs, String encoding)
        throws UnsupportedEncodingException
    {
        return decode(bs.getBytes(), encoding);
    }

    public static String decode(byte[] bytes, String encoding)
        throws UnsupportedEncodingException
    {
        return new String(bytes, encoding);
    }

    public String getDefaultEncoding()
    {
        return defaultEncoding;
    }

    public void setDefaultEncoding(String encoding)
    {
        defaultEncoding = encoding;
    }

    public String decode(String encoding)
        throws UnsupportedEncodingException
    {
        return new String(bytes, encoding);
    }

    public String toString(String encoding)
        throws UnsupportedEncodingException
    {
        // Remove trailing NULL character
        return decode(encoding).replace("\0", "");
    }

    public static String toString(byte[] bytes, String encoding)
        throws UnsupportedEncodingException
    {
        return decode(bytes, encoding);
    }

    public String toString()
    {
        String ret = "";

        try
        {
            ret = toString(defaultEncoding);
        }
        catch(UnsupportedEncodingException e)
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
        return compare(bytes,((ByteString)o).getBytes());
    }

    public boolean equals(Object o)
    {
        if(o instanceof ByteString) {
            return Arrays.equals(bytes, ((ByteString)o).getBytes());
        } else {
            return false;
        }
    }

    public int hashCode()
    {
        return Arrays.hashCode(bytes);
    }
}
