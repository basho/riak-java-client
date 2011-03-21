package com.basho.riak.client.util;

public class ByteUtils {
	
	public static byte[] concat(byte[] ... arrays) {
    	int size = 0;
    	for(byte[] array: arrays) {
    		size += array.length;
    	}
    	
    	int index = 0;
    	byte[] result = new byte[size];
    	for(byte[] array: arrays) {
    		System.arraycopy(array, 0, result, index, array.length);
    		index += array.length;
    	}
    	return result;
	}

}
