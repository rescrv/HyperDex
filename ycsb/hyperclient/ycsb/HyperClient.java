/**
 * Copyright (c) 2011, Cornell University
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

/* Descriptions borrowed from YCSB base. */

package hyperclient.ycsb;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.*;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import hyperclient.*;

public class HyperClient extends DB
{
    private client m_client;
    private Pattern m_pat;
    private Matcher m_mat;
    private boolean m_scannable;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException
    {
        String host = getProperties().getProperty("hyperclient.host", "127.0.0.1");
        Integer port = Integer.parseInt(getProperties().getProperty("hyperclient.port", "1234"));

        location loc = new location(host, port);
        m_client = new client(loc);
        int ret = 0;

        while ((ret = m_client.connect()) != 0)
        {
            System.err.println("Could not connect to HyperDex coordinator (code: " + ret + ") ... sleeping");

            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
            }
        }

        m_pat = Pattern.compile("([a-zA-Z]*)([0-9]*)");
        m_mat = m_pat.matcher("user1");
        m_scannable = getProperties().getProperty("hyperclient.scannable", "false").equals("true");
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields, HashMap<String,String> result)
    {
        int ret = 0;
        mapstrbuf res = new mapstrbuf();

        for (int i = 0; i < 6; ++i)
        {
            ret = m_client.get(table, new buffer(key), res);

            if (ret == 0)
            {
                convert(res, result);
                return 0;
            }
        }

        System.err.println("READ returned " + ret);
        return ret;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,String>> result)
    {
        if (!m_scannable)
        {
            System.err.println("SCAN returned " + 1000);
            return 1000;
        }

        // XXX I'm going to be lazy and not support "fields" for now.  Patches
        // welcome.
        int ret = 0;
        searchresult res = new searchresult();
        mapstrbuf e = new mapstrbuf();
        rangesearch r = new rangesearch();
        m_mat.reset(startkey);

        if (!m_mat.matches())
        {
            System.err.println("SCAN returned " + 1001);
            return 1001;
        }

        e.set("username", new buffer(m_mat.group(1)));
        BigInteger bi = new BigInteger(m_mat.group(2));
        r.set("recno", new rangepair(bi, bi.add(new BigInteger("1"))));

        for (int i = 0; i < 6; ++i)
        {
            ret = m_client.search(table, e, r, res);

            if (ret == 0)
            {
                result.removeAllElements();

                for (int j = 0; j < res.size(); ++j)
                {
                    HashMap<String,String> one = new HashMap<String,String>();
                    convert(res.get(j), one);
                    result.add(one);
                }

                return 0;
            }
        }

        System.err.println("SCAN returned " + ret);
        return ret;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String,String> values)
    {
        mapstrbuf val = new mapstrbuf();

        for (Map.Entry<String, String> entry : values.entrySet())
        {
            val.set(entry.getKey(), new buffer(entry.getValue()));
        }

        if (m_scannable)
        {
            m_mat.reset(key);

            if (!m_mat.matches())
            {
                System.err.println("UPDATE returned " + 1000);
                return 1000;
            }

            val.set("username", new buffer(m_mat.group(1)));
            BigInteger bi = new BigInteger(m_mat.group(2));
            byte[] be = bi.toByteArray();
            byte[] le = new byte[8];

            for (int i = 0; i < 8; ++i)
            {
                le[i] = 0;
            }

            for (int i = 0; i < be.length; ++i)
            {
                le[8 - i - 1] = be[i];
            }

            val.set("recno", new buffer(le, le.length));
        }

        int ret = 0;

        for (int i = 0; i < 6; ++i)
        {
            ret = m_client.put(table, new buffer(key), val);

            if (ret == 0)
            {
                return 0;
            }
        }

        System.err.println("UPDATE returned " + ret);
        return ret;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, HashMap<String,String> values)
    {
        return update(table, key, values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key)
    {
        int ret = 0;

        for (int i = 0; i < 6; ++i)
        {
            ret = m_client.del(table, new buffer(key));

            if (ret == 0)
            {
                return 0;
            }
        }

        System.err.println("DELETE returned " + ret);
        return ret;
    }

    private void convert(mapstrbuf val, HashMap<String,String> value)
    {
        if (val.has_key("key")) { value.put("key", val.get("key").str()); }
        if (val.has_key("username")) { value.put("username", val.get("username").str()); }
        if (val.has_key("recno")) { value.put("recno", val.get("recno").str()); }
        if (val.has_key("field0")) { value.put("field0", val.get("field0").str()); }
        if (val.has_key("field1")) { value.put("field1", val.get("field1").str()); }
        if (val.has_key("field2")) { value.put("field2", val.get("field2").str()); }
        if (val.has_key("field3")) { value.put("field3", val.get("field3").str()); }
        if (val.has_key("field4")) { value.put("field4", val.get("field4").str()); }
        if (val.has_key("field5")) { value.put("field5", val.get("field5").str()); }
        if (val.has_key("field6")) { value.put("field6", val.get("field6").str()); }
        if (val.has_key("field7")) { value.put("field7", val.get("field7").str()); }
        if (val.has_key("field8")) { value.put("field8", val.get("field8").str()); }
        if (val.has_key("field9")) { value.put("field9", val.get("field9").str()); }
    }
}
