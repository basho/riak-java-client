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
package com.basho.riak.newapi.builders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.basho.riak.newapi.RiakLink;
import com.basho.riak.newapi.RiakObject;
import com.basho.riak.newapi.IRiakObject;
import com.basho.riak.newapi.cap.BasicVClock;
import com.basho.riak.newapi.cap.VClock;

/**
 * @author russell
 * 
 */
public class RiakObjectBuilder {
    private final String bucket;
    private final String key;
    private String value;
    private VClock vclock;
    private String vtag;
    private Date lastModified;
    private Collection<RiakLink> links = new ArrayList<RiakLink>();
    private Map<String, String> userMeta = new HashMap<String, String>();
    private String contentType;

    private RiakObjectBuilder(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    public static RiakObjectBuilder newBuilder(String bucket, String key) {
        return new RiakObjectBuilder(bucket, key);
    }

    public static RiakObjectBuilder from(IRiakObject o) {
        RiakObjectBuilder rob = new RiakObjectBuilder(o.getBucket(), o.getKey());
        rob.vclock = o.getVClock();
        rob.contentType = o.getContentType();
        rob.lastModified = o.getLastModified();
        rob.value = o.getValue();
        rob.links = o.getLinks();
        rob.userMeta = o.getMeta();
        return rob;
    }

    public IRiakObject build() {
        return new RiakObject(bucket, key, vclock, vtag, lastModified, contentType, value, links, userMeta);
    }

    public RiakObjectBuilder withValue(String value) {
        this.value = value;
        return this;
    }

    public RiakObjectBuilder withVClock(byte[] vclock) {
        this.vclock = new BasicVClock(vclock);
        return this;
    }

    public RiakObjectBuilder withVtag(String vtag) {
        this.vtag = vtag;
        return this;
    }

    public RiakObjectBuilder withLastModified(long lastModified) {
        this.lastModified = new Date(lastModified);
        return this;
    }

    public RiakObjectBuilder withLinks(Collection<RiakLink> links) {
        if(links != null) {
            this.links = new ArrayList<RiakLink>(links);
        }
        return this;
    }

    public RiakObjectBuilder addLink(String bucket, String key, String tag) {
        synchronized (links) {
            links.add(new RiakLink(bucket, key, tag));
        }
        return this;
    }

    public RiakObjectBuilder withUsermeta(Map<String, String> usermeta) {
        if(usermeta != null) {
            this.userMeta = new HashMap<String, String>(usermeta);
        }
        return this;
    }

    public RiakObjectBuilder addUsermeta(String key, String value) {
        synchronized (userMeta) {
            userMeta.put(key, value);
        }
        return this;
    }

    public RiakObjectBuilder withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public RiakObjectBuilder withVClock(VClock vclock) {
        this.vclock = vclock;
        return this;
    }

}
