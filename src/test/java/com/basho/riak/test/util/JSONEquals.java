/**
 * 
 */
package com.basho.riak.test.util;

import java.util.Arrays;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Since neither {@link JSONObject} nor {@link JSONArray} provide equals.
 * 
 * Equals is needed since we can't guarantee order of elements in a JSONObject so we can't compare
 * JSONObject.toString() with an expected String representation of a JSONObject.
 * 
 * 
 * @author russell
 *
 */
public class JSONEquals {
    
    private JSONEquals() {}

    /**
     * Compare two {@link JSONObject}s for equality.
     * 
     * Warning: This method assumes that the data structure is acyclical (just like {@link JSONObject#toString()}
     * 
     * @param json1 {@link JSONObject} to compare with json2
     * @param json2 {@link JSONObject} to compare with json1
     * @return true if both objects contain exactly the same elements.
     * @throws JSONException
     */
    public static boolean equals(final JSONObject json1, final JSONObject json2) throws JSONException {
        boolean equal = false;
        
        if(andNull(json1, json2)) {
            equal = true;
        } else if(xorNull(json1, json2)) {
            equal = false;
        } else if(json1.equals(json2)) {
            equal = true;
        } else if (json1.length() != json2.length()) {
            equal = false;
        } else {
            equal = logicallyEqual(json1, json2);
        }
        
        return equal;
    }

    /**
     * Is only one of the passed objects null?
     * @param o1
     * @param o2
     * @return true if either o1 or o2 (and not both) are null
     */
    private static boolean xorNull(Object o1, Object o2) {
        return o1 == null ^ o2 == null;
    }

    /**
     * Are both the objects null?
     * @param o1
     * @param o2
     * @return true if both o1 and o2 are null
     */
    private static boolean andNull(Object o1, Object o2) {
        return o1 == null && o2 == null;
    }

    /**
     * Tests the two {@link JSONObject}s for logical equality (Same elements, same values).
     * 
     * Expects that 2 non null {@link JSONObject}s of the same size.
     * @param json1
     * @param json2
     * @return true if both {@link JSONObject}s contain exactly the same elements and they are all equal.
     * @throws JSONException
     */
    private static boolean logicallyEqual(JSONObject json1, JSONObject json2) throws JSONException {
        boolean equal = true;
        @SuppressWarnings("rawtypes") Iterator iterator = json1.keys();
        
        while(iterator.hasNext()){
            String key = (String)iterator.next();
            
            if(!json2.has(key)) {
                equal = false;
                break;
            }
            
            Object o1 = json1.get(key);
            Object o2 = json2.get(key);
            
           if(!objectsEqual(o1, o2)) {
               equal = false;
               break;
           }
        }
        
        return equal;
    }

    /**
     * Tests if two objects are equal. If the objects are neither {@link JSONObject} nor {@link JSONArray}
     * delegates to o1.equals(o2).
     * 
     * @param o1
     * @param o2
     * @return true if the objects are equal, false otherwise
     * @throws JSONException
     */
    private static boolean objectsEqual(Object o1, Object o2) throws JSONException {
        boolean equal = false;
        
        if(o1 instanceof JSONObject && o2 instanceof JSONObject) {
            equal = JSONEquals.equals((JSONObject)o1, (JSONObject)o2);
        } else if(o1 instanceof JSONArray && o2 instanceof JSONArray) {
            equal = JSONEquals.equals((JSONArray)o1, (JSONArray)o2);
        } else if (o1.getClass().isArray() && o2.getClass().isArray()) {
            equal = Arrays.deepEquals((Object[]) o1, (Object[]) o2);
        } else {
            equal = o1.equals(o2);
        }
        return equal;
    }

    /**
     * Compare two {@link JSONArray}s for equality.
     * 
     * Warning: This method assumes that the data structure is acyclical (just like {@link JSONArray#toString()}
     * 
     * @param array1 {@link JSONArray} to compare with array2
     * @param array2 {@link JSONArray} to compare with array1
     * @return true if both objects contain exactly the same elements.
     * @throws JSONException
     */
    public static boolean equals(final JSONArray array1, final JSONArray array2) throws JSONException {
        boolean equal = false;
        
        if(andNull(array1, array2)) {
            equal = true;
        } else if(xorNull(array1, array2)) {
            equal = false;
        } else if(array1.equals(array2)) {
            equal = true;
        } else if(array1.length() != array2.length()) {
            equal = false;
        } else {
            equal = logicallyEqual(array1, array2);
        }
        
        return equal;
    }

    /**
     * Tests the two {@link JSONArray}s for logical equality same values in same order.
     * 
     * Expects that 2 non null {@link JSONArray}s of the same size.
     * @param array1
     * @param array2
     * @return true if both {@link JSONArray}s contain exactly the same elements in the same order.
     * @throws JSONException
     */
    private static boolean logicallyEqual(JSONArray array1, JSONArray array2) throws JSONException {
        boolean equal = true;
        
        for(int i=0; i < array1.length(); i++) {
            Object o1 = array1.get(i);
            Object o2 = array2.get(i);
            
            if(!objectsEqual(o1, o2)) {
                equal = false;
                break;
            }
        }
        
        return equal;
    }
    
}
