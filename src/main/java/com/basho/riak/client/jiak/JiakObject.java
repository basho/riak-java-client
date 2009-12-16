package com.basho.riak.client.jiak;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.util.Constants;


public class JiakObject implements RiakObject {

    private String bucket; 
    private String key;
    private JSONObject value;
    private JSONArray links;
    private String vclock;
    private String lastmod;
    private String vtag;
    private JSONObject usermeta;

    public JiakObject(JSONObject object) throws JSONException {
        this(object.getString(Constants.JIAK_BUCKET),
                object.getString(Constants.JIAK_KEY),
                object.optJSONObject(Constants.JIAK_VALUE),
                object.optJSONArray(Constants.JIAK_LINKS),
                object.optJSONObject(Constants.JIAK_USERMETA),
                object.optString(Constants.JIAK_VCLOCK),
                object.optString(Constants.JIAK_LAST_MODIFIED),
                object.optString(Constants.JIAK_VTAG));
    }
    public JiakObject(String bucket, String key) {
        this(bucket, key, new JSONObject(), new JSONArray(), null, null, null, null);
    }
    public JiakObject(String bucket, String key, JSONObject value) {
        this(bucket, key, value, new JSONArray(), null, null, null, null);
    }
    public JiakObject(String bucket, String key, JSONObject value, JSONArray links) {
        this(bucket, key, value, links, null, null, null, null);
    }
    public JiakObject(String bucket, String key, JSONObject value, JSONArray links, JSONObject usermeta) {
        this(bucket, key, value, links, usermeta, null, null, null);
    }
    public JiakObject(String bucket, String key, JSONObject value, JSONArray links, JSONObject usermeta, String vclock, String lastmod, String vtag) {
        this.bucket = bucket;
        this.key = key;
        this.value = value;
        this.links = links;
        this.usermeta = usermeta;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;
    }

    public String getBucket() { 
        return bucket; 
    } 

    public String getKey() { 
        return key; 
    }

    public String getValue() { 
        if (this.value == null)
            return null;
        return this.value.toString(); 
    }

    public JSONObject getValueAsJSON() { 
        return value; 
    }
    
    public void setValue(JSONObject object) {
        if (object == null)
            object = new JSONObject();
        this.value = object;
    }

    public Object get(String key) {
        return this.value.opt(key);
    }
    
    public void set(String key, Object value) {
        if (value != null) {
            try {
                this.value.put(key, value);
            } catch (JSONException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
    
    public Collection<RiakLink> getLinks() { 
        List<RiakLink> links = new ArrayList<RiakLink>();
        if (links != null && this.links.length() > 0) {
            for (int i = 0; i < this.links.length(); i++) {
                try {
                    JSONArray link = this.links.getJSONArray(i);
                    links.add(new RiakLink(
                            link.getString(0),   // bucket 
                            link.getString(1),   // key
                            link.getString(2))); // tag
                } catch (JSONException e) { }
            }
        }
        return links;
    }

    public JSONArray getLinksAsJSON() {
        return this.links;
    }
    
    public void setLinks(JSONArray links) {
        if (links == null)
            links = new JSONArray();
        this.links = links;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getUsermeta() {
        if (usermeta == null)
            return null;

        Map<String, String> usermeta = new HashMap<String, String>();
        for (Iterator<Object> iter = this.usermeta.keys(); iter.hasNext(); ) {
            String key = iter.next().toString();
            usermeta.put(key, this.usermeta.optString(key));
        }
        
        return usermeta;
    }
    
    public JSONObject getUsermetaAsJSON() {
        return this.usermeta;
    }
    
    public void setUserMeta(JSONObject usermeta) {
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

    public String getEntity() {
        return this.toJSONString();
    }

    public InputStream getEntityStream() {
        if (this.value == null)
            return null;
        return new ByteArrayInputStream(this.getEntity().getBytes());
    }

    public long getEntityStreamLength() {
        if (this.value == null)
            return 0;
        return this.getEntity().getBytes().length;
    }

    public JSONObject toJSONObject() {
        JSONObject o = new JSONObject();
        try {
            if (getVclock() != null)
                o.put("vclock", getVclock());
            if (getLastmod() != null)
                o.put("lastmod", getLastmod());
            if (getVtag() != null)
                o.put("vtag", getVtag());
            if (getBucket() != null)
                o.put("bucket", getBucket());
            if (getKey() != null)
                o.put("key", getKey());
            if (getLinks() != null)
                o.put("links", getLinksAsJSON());
            if (getValueAsJSON() != null)
                o.put("object", getValueAsJSON());
        } catch (JSONException e) {
            throw new IllegalStateException(e);
        }
        return o;
    }
    
    public String toJSONString() {
        return toJSONObject().toString();
    }
}
