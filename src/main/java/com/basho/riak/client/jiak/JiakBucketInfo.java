package com.basho.riak.client.jiak;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.util.Constants;

/**
 * Adds convenience methods to RiakBucketInfo to set the bucket schema
 * properties specific to the Riak Jiak interface. See Jiak documentation for
 * more information about the meaning of each property.
 */
public class JiakBucketInfo extends RiakBucketInfo {

    public JiakBucketInfo() {
        super(null, null);
    }

    public JiakBucketInfo(JSONObject schema, JSONArray keys) {
        super(schema, keys);
    }

    public void setAllowedFields(List<String> allowedFields) {
        try {
            if (allowedFields == null) {
                getSchema().put(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS, "*");
            } else {
                getSchema().put(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS, allowedFields);
            }
        } catch (JSONException unreached) {
            throw new IllegalStateException("can always add strings and list<string> to json object", unreached);
        }
    }

    public void setRequiredFields(List<String> requiredFields) {
        try {
            if (requiredFields == null) {
                getSchema().put(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS, new ArrayList<String>());
            } else {
                getSchema().put(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS, requiredFields);
            }
        } catch (JSONException unreached) {
            throw new IllegalStateException("can always add strings and list<string> to json object", unreached);
        }
    }

    public void setWriteMask(List<String> writeMask) {
        try {
            if (writeMask == null) {
                getSchema().put(Constants.JIAK_FL_SCHEMA_WRITE_MASK, "*");
            } else {
                getSchema().put(Constants.JIAK_FL_SCHEMA_WRITE_MASK, writeMask);
            }
        } catch (JSONException unreached) {
            throw new IllegalStateException("can always add strings and list<string> to json object", unreached);
        }

    }

    public void setReadMask(List<String> readMask) {
        try {
            if (readMask == null) {
                getSchema().put(Constants.JIAK_FL_SCHEMA_READ_MASK, "*");
            } else {
                getSchema().put(Constants.JIAK_FL_SCHEMA_READ_MASK, readMask);
            }
        } catch (JSONException unreached) {
            throw new IllegalStateException("can always add strings and list<string> to json object", unreached);
        }
    }
}
