package com.basho.riak.client;

import java.util.ArrayList;
import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONObject;

import com.basho.riak.client.util.ClientUtils;

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

    /**
     * Construct a bucket info to populate in a writeSchema request.
     */
    public RiakBucketInfo() {
        this(null, null);
    }
    
    /**
     * Construct a bucket info using the JSON data from a listBucket() response.
     *  
     * @param schema The JSON object containing the bucket's schema
     * @param keys The JSON array containing the keys in the bucket  
     */
    public RiakBucketInfo(JSONObject schema, JSONArray jsonKeys) {

        Collection<String> keys = ClientUtils.jsonArrayAsList(jsonKeys);

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
