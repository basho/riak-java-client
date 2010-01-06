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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.ClientUtils;
import com.basho.riak.client.util.Constants;

/**
 * Implementation of RiakObject which interprets objects retrieved from Riak's
 * Jiak interface. Internally, the value, links, and user-defined metadata are
 * all stored as JSON objects and converted to their Java object equivalents on
 * request.
 */
public class JiakObject implements RiakObject {

    private String bucket;
    private String key;
    private JSONObject value;
    private List<RiakLink> links;
    private Map<String, String> usermeta;
    private String vclock;
    private String lastmod;
    private String vtag;

    /**
     * Build a JiakObject from existing JSON. Throws {@link JSONException} if
     * any required fields (bucket or key) are missing.
     * 
     * @param object
     *            JSON representation of the Jiak object
     * @throws JSONException
     *             If bucket or key fields are missing in the JSON
     */
    public JiakObject(JSONObject object) throws JSONException {
        this(object.getString(Constants.JIAK_FL_BUCKET), object.getString(Constants.JIAK_FL_KEY),
             object.optJSONObject(Constants.JIAK_FL_VALUE), null, null, object.optString(Constants.JIAK_FL_VCLOCK),
             object.optString(Constants.JIAK_FL_LAST_MODIFIED), object.optString(Constants.JIAK_FL_VTAG));

        this.setLinks(object.optJSONArray(Constants.JIAK_FL_LINKS));

        JSONObject value = object.optJSONObject(Constants.JIAK_FL_VALUE);
        if (value != null) {
            this.setUsermeta(ClientUtils.jsonObjectAsMap(value.optJSONObject(Constants.JIAK_FL_USERMETA)));
        }
    }

    public JiakObject(String bucket, String key) {
        this(bucket, key, null, null, null, null, null, null);
    }

    public JiakObject(String bucket, String key, JSONObject value) {
        this(bucket, key, value, null, null, null, null, null);
    }

    public JiakObject(String bucket, String key, JSONObject value, List<RiakLink> links) {
        this(bucket, key, value, links, null, null, null, null);
    }

    public JiakObject(String bucket, String key, JSONObject value, List<RiakLink> links,
            Map<String, String> usermeta) {
        this(bucket, key, value, links, usermeta, null, null, null);
    }

    public JiakObject(String bucket, String key, JSONObject value, List<RiakLink> links,
            Map<String, String> usermeta, String vclock, String lastmod, String vtag) {
        this.bucket = bucket;
        this.key = key;
        this.value = value;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;

        setLinks(links);
        setUsermeta(usermeta);
    }

    public void copyData(RiakObject object) {
        if (object == null)
            return;

        if (object.getValue() != null) {
            try {
                value = new JSONObject(object.getValue());
            } catch (JSONException e) {
                try {
                    value = new JSONObject().put("v", object.getValue());
                } catch (JSONException unreached) {
                    throw new IllegalStateException("can always add non-null string to json", unreached);
                }
            }
        } else {
            value = new JSONObject();
        }
        links = new ArrayList<RiakLink>();
        if (object.getLinks() != null) {
            for (RiakLink link : object.getLinks()) {
                links.add(new RiakLink(link));
            }
        }
        usermeta = new HashMap<String, String>();
        if (object.getUsermeta() != null) {
            usermeta.putAll(object.getUsermeta());
        }
        vclock = object.getVclock();
        lastmod = object.getLastmod();
        vtag = object.getVtag();
    }

    public void updateMeta(StoreResponse response) {
        if (response == null)
            return;
        vclock = response.getVclock();
        lastmod = response.getLastmod();
        vtag = response.getVtag();
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        if (value == null)
            return null;
        return value.toString();
    }

    public JSONObject getValueAsJSON() {
        return value;
    }

    public void setValue(String json) {
        if (json == null) {
            value = new JSONObject();
        } else {
            try {
                value = new JSONObject(json);
            } catch (JSONException e) {
                throw new IllegalArgumentException("JiakObject value must be valid JSON", e);
            }
        }
    }

    public void setValue(JSONObject object) {
        if (object == null) {
            object = new JSONObject();
        }
        value = object;
    }

    /**
     * @return the value associated with this key in this object's value (not
     *         metadata)
     */
    public Object get(String key) {
        return value.opt(key);
    }

    /**
     * @return set the value of some key in this object's value (not metadata)
     * @throws IllegalArgumentException
     *             if <code>value</code> cannot be added to valid JSON
     */
    public void set(String key, Object value) {
        if (value != null) {
            try {
                this.value.put(key, value);
            } catch (JSONException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public List<RiakLink> getLinks() {
        return links;
    }

    /**
     * @return The object's links serialized as JSON. A link in Jiak consists of
     *         an array with three elements: the target bucket, target key, and
     *         link tag. Links added to this array will not be included in the
     *         Jiak object.
     */
    public JSONArray getLinksAsJSON() {
        JSONArray jsonLinks = new JSONArray();
        for (RiakLink link : links) {
            JSONArray jsonLink = new JSONArray();
            jsonLink.put(link.getBucket());
            jsonLink.put(link.getKey());
            jsonLink.put(link.getTag());
            jsonLinks.put(jsonLink);
        }
        return jsonLinks;
    }

    public void setLinks(List<RiakLink> links) {
        if (links == null) {
            links = new ArrayList<RiakLink>();
        }
        this.links = links;
    }

    void setLinks(JSONArray json) {
        links = new ArrayList<RiakLink>();

        if (json != null) {
            for (int i = 0; i < json.length(); i++) {
                JSONArray jsonLink = json.optJSONArray(i);
                links.add(new RiakLink(jsonLink.optString(0), // bucket
                                       jsonLink.optString(1), // key
                                       jsonLink.optString(2))); // tag
            }
        }
    }

    public Map<String, String> getUsermeta() {
        return usermeta;
    }

    /**
     * @return The usermeta map serialized to a JSONObject. Data added to this object will not be included in the
     *         Jiak object.
     */
    public JSONObject getUsermetaAsJSON() {
        JSONObject jsonUsermeta = new JSONObject();
        for (String key : usermeta.keySet()) {
            try {
                jsonUsermeta.put(key, usermeta.get(key));
            } catch (JSONException unreached) {
                throw new IllegalStateException("can always add non-null string to json", unreached);
            }
        }
        return jsonUsermeta;
    }

    /**
     * Jiak does not currently support extra user-defined metadata. It only
     * stores links and the object value. Anything set here will be added to the
     * "usermeta" field of the value.
     */
    public void setUsermeta(Map<String, String> usermeta) {
        if (usermeta == null) {
            usermeta = new HashMap<String, String>();
        }
        this.usermeta = usermeta;
    }

    public String getContentType() {
        return Constants.CTYPE_JSON;
    }

    public String getVclock() {
        return vclock;
    }

    public String getLastmod() {
        return lastmod;
    }

    public String getVtag() {
        return vtag;
    }

    public void writeToHttpMethod(HttpMethod httpMethod) {
        // Jiak stores the object value and metadata all in the message body.
        if (httpMethod instanceof EntityEnclosingMethod) {
            ((EntityEnclosingMethod) httpMethod).setRequestEntity(new ByteArrayRequestEntity(toJSONString().getBytes(),
                                                                                             Constants.CTYPE_JSON));
        }
    }

    public JSONObject toJSONObject() {
        JSONObject o = new JSONObject();
        try {
            JSONObject value = getValueAsJSON();
            JSONObject usermeta = getUsermetaAsJSON();
            JSONArray links = getLinksAsJSON();

            if (usermeta.keys().hasNext()) {
                if (value == null) {
                    value = new JSONObject().put(Constants.JIAK_FL_USERMETA, usermeta);
                } else {
                    value.put(Constants.JIAK_FL_USERMETA, usermeta);
                }
            }
            if (value != null) {
                o.put(Constants.JIAK_FL_VALUE, value);
            } else {
                o.put(Constants.JIAK_FL_VALUE, new JSONObject());
            }
            if (getBucket() != null) {
                o.put(Constants.JIAK_FL_BUCKET, getBucket());
            } else {
                o.put(Constants.JIAK_FL_BUCKET, "");
            }
            if (getKey() != null) {
                o.put(Constants.JIAK_FL_KEY, getKey());
            } else {
                o.put(Constants.JIAK_FL_BUCKET, "");
            }
            if (links != null) {
                o.put(Constants.JIAK_FL_LINKS, links);
            } else {
                o.put(Constants.JIAK_FL_BUCKET, new JSONArray());
            }
            if (getVclock() != null) {
                o.put(Constants.JIAK_FL_VCLOCK, getVclock());
            }
            if (getLastmod() != null) {
                o.put(Constants.JIAK_FL_LAST_MODIFIED, getLastmod());
            }
            if (getVtag() != null) {
                o.put(Constants.JIAK_FL_VTAG, getVtag());
            }
        } catch (JSONException e) {
            throw new IllegalStateException("can always add non null strings and json objects to json", e);
        }
        return o;
    }

    public String toJSONString() {
        return toJSONObject().toString();
    }
}
