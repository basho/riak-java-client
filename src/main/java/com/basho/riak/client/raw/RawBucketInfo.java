package com.basho.riak.client.raw;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.util.Constants;

/**
 * Adds convenience methods to RiakBucketInfo to set the bucket schema
 * properties specific to the Riak Raw interface
 */
public class RawBucketInfo extends RiakBucketInfo {

    public RawBucketInfo() {
        super(null, null);
    }
    
    public RawBucketInfo(JSONObject schema, JSONArray keys) {
        super(schema, keys);
    }

    /**
     * Allow siblings to be returned for an object. If false, last write wins.
     */
    public void setAllowMult(boolean allowMult) {
        try {
            getSchema().put(Constants.RAW_FL_SCHEMA_ALLOW_MULT, allowMult);
        } catch (JSONException unreached) {
            throw new IllegalStateException("operation is valid json", unreached);
        }
    }

    /**
     * Erlang module and name of the function to use to hash object keys. See
     * Riak's Raw interface documentation.
     */
    public void setCHashFun(String mod, String fun) {
        if (mod == null) {
            mod = "";
        }
        if (fun == null) {
            fun = "";
        }
        try {
            JSONObject chashfun = new JSONObject();
            chashfun.put(Constants.RAW_FL_SCHEMA_CHASHFUN_MOD, mod);
            chashfun.put(Constants.RAW_FL_SCHEMA_CHASHFUN_FUN, fun);
            getSchema().put(Constants.RAW_FL_SCHEMA_CHASHFUN, chashfun);
        } catch (JSONException unreached) {
            throw new IllegalStateException("operation is valid json", unreached);
        }
    }

    /**
     * Erlang module and name of the function to use to walk object links. See
     * Riak's Raw interface documentation.
     */
    public void setLinkFun(String mod, String fun) {
        if (mod == null) {
            mod = "";
        }
        if (fun == null) {
            fun = "";
        }
        try {
            JSONObject linkfun = new JSONObject();
            linkfun.put(Constants.RAW_FL_SCHEMA_LINKFUN_MOD, mod);
            linkfun.put(Constants.RAW_FL_SCHEMA_LINKFUN_FUN, fun);
            getSchema().put(Constants.RAW_FL_SCHEMA_LINKFUN, linkfun);
        } catch (JSONException unreached) {
            throw new IllegalStateException("operation is valid json", unreached);
        }
    }

    /**
     * Number of replicas per object in this bucket.
     */
    public void setNVal(int n) {
        try {
            getSchema().put(Constants.RAW_FL_SCHEMA_NVAL, n);
        } catch (JSONException unreached) {
            throw new IllegalStateException("operation is valid json", unreached);
        }
    }
}
