package com.basho.riak.client;

import java.util.ArrayList;
import java.util.Collection;

import org.json.JSONObject;

/**
 * Represents the metadata stored in a bucket including its schema (whose fields
 * are interface dependent) and the list of keys contained in the bucket.
 */
public class RiakBucketInfo {

    private JSONObject schema;
    private Collection<String> keys;

    /**
     * Returns the list of properties in the schema and their values. The
     * properties available is interface dependent.
     * 
     * @return The properties composing this bucket's schema.
     */
    public JSONObject getSchema() {
        return schema;
    }

    /**
     * @return The object keys in this bucket.
     */
    public Collection<String> getKeys() {
        return keys;
    }

    public RiakBucketInfo(JSONObject schema, Collection<String> keys) {
        if (schema != null) {
            this.schema = schema;
        } else {
            this.schema = new JSONObject();
        }
        if (keys != null) {
            this.keys = keys;
        } else {
            this.keys = new ArrayList<String>();
        }
    }
}
