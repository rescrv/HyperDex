/* This example uses the following space definition */
/*
space alltypes
dimensions keystr, str, int (int64), flt (float), list_str (list (string)), list_int (list (int64)), list_flt (list (float)), set_str (set (string)), set_int (set (int64)), set_flt (set  (float)), str2str (map (string, string)), str2int (map (string, int64)), str2flt (map (string, float)), int2str (map (int64, string)), int2int (map (int64, int64)), int2flt (map (int64, float)), flt2str (map (float, string)), flt2int (map (float, int64)), flt2flt (map (float, float))
key keystr auto 1 3
*/


import hyperclient.*;
import java.util.*;

public class HyperAllTypes
{
	public static void main(String[] args) throws Exception
	{
        HashMap<String,Object> attrs = new HashMap<String,Object>();

        // Primitive types
        //
        attrs.put("str","Hello");
        attrs.put("int", 42L);
        attrs.put("flt", 3.14D);

        // List types
        //
        Vector<String> list_str = new Vector<String>();
        list_str.add("dog");
        list_str.add("cat");
        list_str.add("horse");
        attrs.put("list_str", list_str);

        Vector<Long> list_int = new Vector<Long>();
        list_int.add(1L);
        list_int.add(2L);
        list_int.add(3L);
        attrs.put("list_int", list_int);

        Vector<Double> list_flt = new Vector<Double>();
        list_flt.add(0.1D);
        list_flt.add(0.01D);
        list_flt.add(0.001D);
        attrs.put("list_flt", list_flt);

        // Set types
        //
        TreeSet<String> set_str = new TreeSet<String>();
        set_str.add("dog");
        set_str.add("cat");
        set_str.add("horse");
        attrs.put("set_str", set_str);

        TreeSet<Long> set_int = new TreeSet<Long>();
        set_int.add(1L);
        set_int.add(2L);
        set_int.add(3L);
        attrs.put("set_int", set_int);

        TreeSet<Double> set_flt = new TreeSet<Double>();
        set_flt.add(0.1D);
        set_flt.add(0.01D);
        set_flt.add(0.001D);
        attrs.put("set_flt", set_flt);

        // Map types
        //
        HashMap<String,String> str2str = new HashMap<String,String>();
        str2str.put("dog", "canine");
        str2str.put("cat", "feline");
        str2str.put("horse", "equine");
        attrs.put("str2str", str2str);

        HashMap<String,Long> str2int = new HashMap<String,Long>();
        str2int.put("one", 1L);
        str2int.put("two", 2L);
        str2int.put("three", 3L);
        attrs.put("str2int", str2int);

        HashMap<String,Double> str2flt = new HashMap<String,Double>();
        str2flt.put("tenth", 0.1);
        str2flt.put("hundredth", 0.01);
        str2flt.put("thousandth", 0.001);
        attrs.put("str2flt", str2flt);

        HashMap<Long,String> int2str = new HashMap<Long,String>();
        int2str.put(1L, "canine");
        int2str.put(2L, "feline");
        int2str.put(3L, "equine");
        attrs.put("int2str", int2str);

        HashMap<Long,Long> int2int = new HashMap<Long,Long>();
        int2int.put(1L, 1L);
        int2int.put(2L, 2L);
        int2int.put(3L, 3L);
        attrs.put("int2int", int2int);

        HashMap<Long,Double> int2flt = new HashMap<Long,Double>();
        int2flt.put(1L, 0.1D);
        int2flt.put(2L, 0.01D);
        int2flt.put(3L, 0.001D);
        attrs.put("int2flt", int2flt);

        HashMap<Double,String> flt2str = new HashMap<Double,String>();
        flt2str.put(0.1D, "canine");
        flt2str.put(0.01D, "feline");
        flt2str.put(0.001D, "equine");
        attrs.put("flt2str", flt2str);

        HashMap<Double,Long> flt2int = new HashMap<Double,Long>();
        flt2int.put(0.1D, 1L);
        flt2int.put(0.01D, 2L);
        flt2int.put(0.001D, 3L);
        attrs.put("flt2int", flt2int);

        HashMap<Double,Double> flt2flt = new HashMap<Double,Double>();
        flt2flt.put(0.1D, 0.1D);
        flt2flt.put(0.01D, 0.01D);
        flt2flt.put(0.001D, 0.001D);
        attrs.put("flt2flt", flt2flt);

		HyperClient c = new HyperClient("127.0.0.1",1234);

		System.out.println("put: " + c.put("alltypes","key1",attrs));
		System.out.println("get: " + c.get("alltypes","key1"));
	}
}
