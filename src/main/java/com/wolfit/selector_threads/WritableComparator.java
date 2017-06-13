package com.wolfit.selector_threads;

import java.io.IOException;
import java.util.HashMap;

public class WritableComparator implements RawComparator {
	private static HashMap<Class, WritableComparator> comparators = 
			new HashMap<Class, WritableComparator>();

    public static synchronized void define(Class c, 
    		WritableComparator comparator) {
        comparators.put(c, comparator);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
    
    public int compare(Object a, Object b) {
        return compare((WritableComparable)a, (WritableComparable)b);
    }

    public static int compareBytes(byte[] b1, int s1, int l1, 
    		byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
          int a = (b1[i] & 0xff);
          int b = (b2[j] & 0xff);
          if (a != b) {
            return a - b;
          }
        }
        
        return l1 - l2;
    }
    
    public static int hashBytes(byte[] bytes, int length) {
        int hash = 1;
        for (int i = 0; i < length; i++) {
        	hash = (31 * hash) + (int)bytes[i];
        }
        
        return hash;
    }
    
    public static int readUnsignedShort(byte[] bytes, int start) {
        return (((bytes[start]   & 0xff) <<  8) +
                ((bytes[start+1] & 0xff)));
    }
    
    public static int readInt(byte[] bytes, int start) {
        return (((bytes[start  ] & 0xff) << 24) +
                ((bytes[start+1] & 0xff) << 16) +
                ((bytes[start+2] & 0xff) <<  8) +
                ((bytes[start+3] & 0xff)));
        
    }
    
    public static float readFloat(byte[] bytes, int start) {
        return Float.intBitsToFloat(readInt(bytes, start));
    }
    
    public static long readLong(byte[] bytes, int start) {
        return ((long)(readInt(bytes, start)) << 32) 
        		+ (readInt(bytes, start+4) & 0xFFFFFFFFL);
    }
    
    public static double readDouble(byte[] bytes, int start) {
        return Double.longBitsToDouble(readLong(bytes, start));
    }
    
    public static long readVLong(byte[] bytes, int start) throws IOException {
        int len = bytes[start];
        if (len >= -112) {
          return len;
        }
        boolean isNegative = (len < -120);
        len = isNegative ? -(len + 120) : -(len + 112);
        if (start+1+len>bytes.length) {
        	throw new IOException("Not enough number of bytes for a zero-compressed integer");
        }
                                
        long i = 0;
        for (int idx = 0; idx < len; idx++) {
            i = i << 8;
            i = i | (bytes[start+1+idx] & 0xFF);
        }
        
        return (isNegative ? (i ^ -1L) : i);
    }
		  
    public static int readVInt(byte[] bytes, int start) throws IOException {
        return (int) readVLong(bytes, start);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    	return 0;
    }
}
