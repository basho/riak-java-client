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
package com.basho.riak.client.raw;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.RiakObject;
import com.basho.riak.client.response.StoreResponse;
import com.basho.riak.client.util.Constants;

/**
 * Implementation of RiakObject which interprets objects retrieved from Riak's
 * Raw interface.
 */
public class RawObject implements RiakObject {

    private String bucket;
    private String key;
    private byte[] value;
    private List<RiakLink> links;
    private Map<String, String> usermeta;
    private String contentType;
    private String vclock;
    private String lastmod;
    private String vtag;
    private InputStream valueStream;
    private Long valueStreamLength;

    /**
     * Create an empty Raw object. The content type defaults to
     * application/octet-stream.
     * 
     * @param bucket
     *            The object's bucket
     * @param key
     *            The object's key
     */
    public RawObject(String bucket, String key) {
        this(bucket, key, null, null, null, null, null, null, null);
    }

    public RawObject(String bucket, String key, String value) {
        this(bucket, key, value, null, null, null, null, null, null);
    }

    public RawObject(String bucket, String key, String value, String contentType) {
        this(bucket, key, value, contentType, null, null, null, null, null);
    }

    public RawObject(String bucket, String key, String value, String contentType, List<RiakLink> links) {
        this(bucket, key, value, contentType, links, null, null, null, null);
    }

    public RawObject(String bucket, String key, String value, String contentType, List<RiakLink> links,
            Map<String, String> usermeta) {
        this(bucket, key, value, contentType, links, usermeta, null, null, null);
    }

    public RawObject(String bucket, String key, String value, String contentType, List<RiakLink> links,
            Map<String, String> usermeta, String vclock, String lastmod, String vtag) {
        this.bucket = bucket;
        this.key = key;
        this.vclock = vclock;
        this.lastmod = lastmod;
        this.vtag = vtag;

        setValue(value);
        setContentType(contentType);
        setLinks(links);
        setUsermeta(usermeta);
    }

    public void copyData(RiakObject object) {
        if (object == null)
            return;

        if (object.getValue() != null) {
            value = object.getValue().getBytes();
        } else {
            value = null;
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
        contentType = object.getContentType();
        vclock = object.getVclock();
        lastmod = object.getLastmod();
        vtag = object.getVtag();
    }

    public void updateMeta(StoreResponse response) {
        if (response == null) {
            vclock = null;
            lastmod = null;
            vtag = null;
        } else {
            vclock = response.getVclock();
            lastmod = response.getLastmod();
            vtag = response.getVtag();
        }
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

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
        if (value != null) {
            this.value = value.getBytes();
        } else {
            this.value = null;
        }
    }

    public List<RiakLink> getLinks() {
        return links;
    }

    public void setLinks(List<RiakLink> links) {
        if (links == null) {
            links = new ArrayList<RiakLink>();
        }
        this.links = links;
    }

    public Map<String, String> getUsermeta() {
        return usermeta;
    }

    public void setUsermeta(Map<String, String> usermeta) {
        if (usermeta == null) {
            usermeta = new HashMap<String, String>();
        }
        this.usermeta = usermeta;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        if (contentType != null) {
            this.contentType = contentType;
        } else {
            this.contentType = Constants.CTYPE_OCTET_STREAM;
        }
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

    /**
     * Set the object's value as a stream. A value set here is independent of
     * and has precedent over values set using setValue(). Calling getValue()
     * will always return values set via setValue(), and calling
     * getValueStream() will always return the stream set via setValueStream.
     * 
     * @param in
     *            Input stream representing the object's value
     * @param len
     *            Length of the InputStream or null if unknown. If null, the
     *            value will be buffered in memory to determine its size before
     *            sending to the server.
     */
    public void setValueStream(InputStream in, Long len) {
        valueStream = in;
        valueStreamLength = len;
    }

    public void setValueStream(InputStream in) {
        valueStream = in;
    }

    public InputStream getValueStream() {
        return valueStream;
    }

    public void setValueStreamLength(Long len) {
        valueStreamLength = len;
    }

    public Long getValueStreamLength() {
        return valueStreamLength;
    }

    public void writeToHttpMethod(HttpMethod httpMethod) {
        // Serialize headers
        String basePath = getBasePathFromHttpMethod(httpMethod);
        StringBuilder linkHeader = new StringBuilder();
        for (RiakLink link : links) {
            if (linkHeader.length() > 0) {
                linkHeader.append(", ");
            }
            linkHeader.append("<");
            linkHeader.append(basePath);
            linkHeader.append("/");
            linkHeader.append(link.getBucket());
            linkHeader.append("/");
            linkHeader.append(link.getKey());
            linkHeader.append(">; ");
            linkHeader.append(Constants.RAW_LINK_TAG);
            linkHeader.append("=\"");
            linkHeader.append(link.getTag());
            linkHeader.append("\"");
        }
        if (linkHeader.length() > 0) {
            httpMethod.setRequestHeader(Constants.HDR_LINK, linkHeader.toString());
        }
        for (String name : usermeta.keySet()) {
            httpMethod.setRequestHeader(Constants.HDR_USERMETA_PREFIX + name, usermeta.get(name));
        }
        if (vclock != null) {
            httpMethod.setRequestHeader(Constants.HDR_VCLOCK, vclock);
        }

        // Serialize body
        if (httpMethod instanceof EntityEnclosingMethod) {
            EntityEnclosingMethod entityEnclosingMethod = (EntityEnclosingMethod) httpMethod;

            // Any value set using setValueAsStream() has precedent over value
            // set using setValue()
            if (valueStream != null) {
                if (valueStreamLength != null && valueStreamLength >= 0) {
                    entityEnclosingMethod.setRequestEntity(new InputStreamRequestEntity(valueStream, valueStreamLength,
                                                                                        contentType));
                } else {
                    entityEnclosingMethod.setRequestEntity(new InputStreamRequestEntity(valueStream, contentType));
                }
            } else if (value != null) {
                entityEnclosingMethod.setRequestEntity(new ByteArrayRequestEntity(value, contentType));
            } else {
                entityEnclosingMethod.setRequestEntity(new ByteArrayRequestEntity("".getBytes(), contentType));
            }
        }
    }

    String getBasePathFromHttpMethod(HttpMethod httpMethod) {
        if (httpMethod == null || httpMethod.getPath() == null)
            return "";

        String path = httpMethod.getPath();
        int idx = path.length() - 1;

        // ignore any trailing slash
        if (path.endsWith("/")) {
            idx--;
        }

        // trim off last two path components
        idx = path.lastIndexOf('/', idx);
        idx = path.lastIndexOf('/', idx - 1);

        if (idx <= 0)
            return "";

        return path.substring(0, idx);
    }

}