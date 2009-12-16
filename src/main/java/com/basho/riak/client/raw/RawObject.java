package com.basho.riak.client.raw;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.util.Constants;


public class RawObject implements RiakObject {

    public RawObject(String bucket, String key) {
        this(bucket, key, null, new ArrayList<RiakLink>(), new HashMap<String, String>(), Constants.CTYPE_OCTET_STREAM, null, null, null);
    }
    public RawObject(String bucket, String key, String value) {
        this(bucket, key, value, new ArrayList<RiakLink>(), new HashMap<String, String>(), Constants.CTYPE_OCTET_STREAM, null, null, null);
    }
    public RawObject(String bucket, String key, String value, Collection<RiakLink> links) {
        this(bucket, key, value, links, new HashMap<String, String>(), Constants.CTYPE_OCTET_STREAM, null, null, null);
    }
    public RawObject(String bucket, String key, String value, Collection<RiakLink> links, Map<String, String> usermeta) {
        this(bucket, key, value, links, usermeta, Constants.CTYPE_OCTET_STREAM, null, null, null);
    }
    public RawObject(String bucket, String key, String value, 
            Collection<RiakLink> links, Map<String, String> usermeta,
            String contentType, String vclock, String lastmod, 
            String vtag) {
        this.bucket = bucket;
        this.key = key;
        if (value != null)
            this.value = value.getBytes();
        if (links != null)
            this.links = links;
        if (usermeta != null)
            this.usermeta = usermeta;
        this.contentType = contentType;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;
    }

    public String getBucket() { return bucket; } 
    private String bucket; 

    public String getKey() { return key; }
    private String key;
    
    public String getValue() { return (this.value == null ? null : new String(this.value)); }
    public byte[] getValueAsBytes() { return this.value; }
    public void setValue(byte[] value) { this.value = value; }
    public void setValue(String value) { this.value = value.getBytes(); }
    private byte[] value = null;
    
    public Collection<RiakLink> getLinks() { return links; }
    public void setLinks(Collection<RiakLink> links) { if (links != null) this.links = links; }
    private Collection<RiakLink> links = new ArrayList<RiakLink>();

    public Map<String, String> getUsermeta() { return usermeta; }
    public void setUsermeta(Map<String, String> usermeta) { if (usermeta != null) this.usermeta = usermeta; }
    private Map<String, String> usermeta = new HashMap<String, String>();

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { if (contentType != null) this.contentType = contentType; }
    private String contentType = Constants.CTYPE_OCTET_STREAM;
    
    public String getVclock() { return this.vclock; }
    private String vclock = null;
    
    public String getLastmod() { return this.lastmod; }
    private String lastmod = null;

    public String getVtag() { return this.vtag; }
    private String vtag = null;

    public String getEntity() { return this.getValue(); }

    public InputStream getEntityStream() { return this.valueStream; }
    public void setValueStream(InputStream in) { this.valueStream = in; }
    public void setValueStream(InputStream in, Long len) { this.valueStream = in; this.valueStreamLength = len; }
    private InputStream valueStream = null;

    public long getEntityStreamLength() { return this.valueStreamLength; }
    public void setValueStreamLength(long len) { this.valueStreamLength = len; }
    private long valueStreamLength = -1;
}