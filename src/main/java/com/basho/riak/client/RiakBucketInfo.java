package com.basho.riak.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata stored in a bucket including its schema (whose fields
 * are interface dependent) and the list of keys contained in the bucket.
 */
public class RiakBucketInfo {

    private Map<String, Object> schema;
    private Collection<String> keys;

    /**
     * Returns the list of properties in the schema and their values. The
     * properties available is interface dependent. In general, the values are
     * JSON and can be parsed by the client into a JSONObject or JSONArray for
     * further inspection.
     * 
     * @return The properties composing this bucket's schema.
     */
    public Map<String, ? extends Object> getSchema() {
        return schema;
    }

    /**
     * Set a specific property in the schema. In general schema values need to
     * be valid JSON to be accepted by Riak. Remember to quote string values.
     * 
     * @param property
     *            Schema property to set
     * @param value
     *            Value of the property; should be valid JSON. Remember to quote
     *            strings.
     */
    public void setSchema(String property, Object value) {
        schema.put(property, value);
    }

    /**
     * Returns the list of properties in the schema. The properties available is
     * interface dependent.
     * 
     * @return The properties in this bucket's schema.
     */
    public Collection<String> getKeys() {
        return keys;
    }

    public RiakBucketInfo(Map<String, ? extends Object> schema, Collection<String> keys) {
        if (schema != null) {
            this.schema = new HashMap<String, Object>(schema);
        } else {
            this.schema = new HashMap<String, Object>();
        }
        if (keys != null) {
            this.keys = keys;
        } else {
            this.keys = new ArrayList<String>();
        }
    }
}
