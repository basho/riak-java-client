/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.jiak;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakBucketInfo;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Adds convenience methods to RiakBucketInfo to set the bucket schema
 * properties specific to the Jiak interface. See Jiak documentation for more
 * information about the meaning of each property.
 */
public class JiakBucketInfo extends RiakBucketInfo {

    public JiakBucketInfo() {
        super(null, null);
    }

    public JiakBucketInfo(JSONObject schema, Collection<String> keys) {
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

    public List<String> getAllowedFields() {
        if ("*".equals(getSchema().optString(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS)))
            return null;

        return ClientUtils.jsonArrayAsList(getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_ALLOWED_FIELDS));
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

    public List<String> getRequiredFields() {
        return ClientUtils.jsonArrayAsList(getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_REQUIRED_FIELDS));
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

    public List<String> getWriteMask() {
        if ("*".equals(getSchema().optString(Constants.JIAK_FL_SCHEMA_WRITE_MASK)))
            return null;

        return ClientUtils.jsonArrayAsList(getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_WRITE_MASK));
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

    public List<String> getReadMask() {
        if ("*".equals(getSchema().optString(Constants.JIAK_FL_SCHEMA_READ_MASK)))
            return null;

        return ClientUtils.jsonArrayAsList(getSchema().optJSONArray(Constants.JIAK_FL_SCHEMA_READ_MASK));
    }
}
