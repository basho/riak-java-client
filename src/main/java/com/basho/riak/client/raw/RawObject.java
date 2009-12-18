/*
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
        this(bucket, key, null, new ArrayList<RiakLink>(), new HashMap<String, String>(), Constants.CTYPE_OCTET_STREAM,
             null, null, null);
    }

    public RawObject(String bucket, String key, String value) {
        this(bucket, key, value, new ArrayList<RiakLink>(), new HashMap<String, String>(),
             Constants.CTYPE_OCTET_STREAM, null, null, null);
    }

    public RawObject(String bucket, String key, String value, Collection<RiakLink> links) {
        this(bucket, key, value, links, new HashMap<String, String>(), Constants.CTYPE_OCTET_STREAM, null, null, null);
    }

    public RawObject(String bucket, String key, String value, Collection<RiakLink> links, Map<String, String> usermeta) {
        this(bucket, key, value, links, usermeta, Constants.CTYPE_OCTET_STREAM, null, null, null);
    }

    public RawObject(String bucket, String key, String value, Collection<RiakLink> links, Map<String, String> usermeta,
            String contentType, String vclock, String lastmod, String vtag) {
        this.bucket = bucket;
        this.key = key;
        if (value != null) {
            this.value = value.getBytes();
        }
        if (links != null) {
            this.links = links;
        }
        if (usermeta != null) {
            this.usermeta = usermeta;
        }
        this.contentType = contentType;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;
    }

    public void copyData(RiakObject object) {
        if (object == null)
            return;

        value = object.getValue().getBytes();
        links = new ArrayList<RiakLink>();
        if (object.getLinks() != null) {
            links.addAll(object.getLinks());
        }
        usermeta = new HashMap<String, String>();
        if (object.getUsermeta() != null) {
            usermeta.putAll(object.getUsermeta());
        }
        contentType = object.getContentType();
        vclock = object.getVclock();
        lastmod = object.getLastmod();
        vtag = object.getVtag();
    }

    public void updateMeta(String vclock, String lastmod, String vtag) {
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;
    }

    public String getBucket() {
        return bucket;
    }

    private String bucket;

    public String getKey() {
        return key;
    }

    private String key;

    public String getValue() {
        return (value == null ? null : new String(value));
    }

    public byte[] getValueAsBytes() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setValue(String value) {
        this.value = value.getBytes();
    }

    private byte[] value = null;

    public Collection<RiakLink> getLinks() {
        return links;
    }

    public void setLinks(Collection<RiakLink> links) {
        if (links != null) {
            this.links = links;
        }
    }

    private Collection<RiakLink> links = new ArrayList<RiakLink>();

    public Map<String, String> getUsermeta() {
        return usermeta;
    }

    public void setUsermeta(Map<String, String> usermeta) {
        if (usermeta != null) {
            this.usermeta = usermeta;
        }
    }

    private Map<String, String> usermeta = new HashMap<String, String>();

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        if (contentType != null) {
            this.contentType = contentType;
        }
    }

    private String contentType = Constants.CTYPE_OCTET_STREAM;

    public String getVclock() {
        return vclock;
    }

    private String vclock = null;

    public String getLastmod() {
        return lastmod;
    }

    private String lastmod = null;

    public String getVtag() {
        return vtag;
    }

    private String vtag = null;

    public String getEntity() {
        return this.getValue();
    }

    public InputStream getEntityStream() {
        return valueStream;
    }

    public void setValueStream(InputStream in) {
        valueStream = in;
    }

    public void setValueStream(InputStream in, Long len) {
        valueStream = in;
        valueStreamLength = len;
    }

    private InputStream valueStream = null;

    public long getEntityStreamLength() {
        return valueStreamLength;
    }

    public void setValueStreamLength(long len) {
        valueStreamLength = len;
    }

    private long valueStreamLength = -1;
}