package com.basho.riak.client.jiak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.util.Constants;

public class JiakBucketInfo extends RiakBucketInfo {

    public JiakBucketInfo(JSONObject schema, Collection<String> keys) {
        super(schema, keys);
    }

    public void setSchema(List<String> allowedFields, List<String> writeMask, List<String> readMask,
                          List<String> requiredFields) {
        
        JSONObject schema = getSchema();

        try {
            if (allowedFields == null) {
                schema.put(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS, "*");
            } else {
                schema.put(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS, allowedFields);
            }
            if (requiredFields == null) {
                schema.put(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS, new ArrayList<String>());
            } else {
                schema.put(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS, requiredFields);
            }
            if (writeMask == null) {
                schema.put(Constants.JIAK_FL_SCHEMA_WRITE_MASK, "*");
            } else {
                schema.put(Constants.JIAK_FL_SCHEMA_WRITE_MASK, writeMask);
            }
            if (readMask == null) {
                schema.put(Constants.JIAK_FL_SCHEMA_READ_MASK, "*");
            } else {
                schema.put(Constants.JIAK_FL_SCHEMA_READ_MASK, readMask);
            }
        } catch (JSONException unreached) {
            throw new IllegalStateException("can always add strings and list<string> to json object", unreached);
        }

    }
}
