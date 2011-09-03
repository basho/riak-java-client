/**
 * This file is part of riak-java-pb-client
 *
 * Copyright (c) 2010 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.basho.riak.pbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.pbc.RPB.RpbContent;
import com.basho.riak.pbc.RPB.RpbPair;
import com.basho.riak.pbc.RPB.RpbContent.Builder;
import com.google.protobuf.ByteString;

/**
 * PBC model of the data/meta data for a bucket/key entry in Riak
 */
public class RiakObject {

	private ByteString vclock;
	private ByteString bucket;
	private ByteString key;
	
	private ByteString value;

	private String contentType;
	private List<RiakLink> links = Collections.synchronizedList(new ArrayList<RiakLink>());
	private String vtag;
	private String contentEncoding;
	private String charset;
	private Object userMetaDataLock = new Object();
	private Map<String,String> userMetaData = new LinkedHashMap<String, String>();
	private Integer lastModified;
	private Integer lastModifiedUsec;
	

	RiakObject(ByteString vclock, ByteString bucket, ByteString key, RpbContent content) {
		this.vclock = vclock;
		this.bucket = bucket;
		this.key = key;
		this.value = content.getValue();
		this.contentType = str(content.getContentType());
		this.charset = str(content.getCharset());
		this.contentEncoding = str(content.getContentEncoding());
		this.vtag = str(content.getVtag());
		this.links = content.getLinksCount() == 0
			? Collections.synchronizedList(new ArrayList<RiakLink>())
			: Collections.synchronizedList(RiakLink.decode(content.getLinksList()));
		
		if (content.hasLastMod()) {
			this.lastModified = new Integer(content.getLastMod());
			this.lastModifiedUsec = new Integer(content.getLastModUsecs());
		}

		if (content.getUsermetaCount() > 0) {
			Map<String, String> tmpUserMetaData = new LinkedHashMap<String, String>();
			for (int i = 0; i < content.getUsermetaCount(); i++) {
				RpbPair um = content.getUsermeta(i);
				tmpUserMetaData.put(um.getKey().toStringUtf8(),
							 str(um.getValue()));
			}

			synchronized (userMetaDataLock) {
			    userMetaData.putAll(tmpUserMetaData);
            }
		}
	}

    public RiakObject(ByteString vclock, ByteString bucket, ByteString key, ByteString content) {
		this.bucket = bucket;
		this.key = key;
		this.value = content;
        this.vclock = vclock;
	}

	public RiakObject(ByteString bucket, ByteString key, ByteString content) {
		this.bucket = bucket;
		this.key = key;
		this.value = content;
	}

	public RiakObject(String bucket, String key, byte[] content) {
		this.bucket = ByteString.copyFromUtf8(bucket);
		this.key = ByteString.copyFromUtf8(key);
		this.value = ByteString.copyFrom(content);
	}
	
	public RiakObject(String bucket, String key, String content) {
		this.bucket = ByteString.copyFromUtf8(bucket);
		this.key = ByteString.copyFromUtf8(key);
		this.value = ByteString.copyFromUtf8(content);
	}

	private String str(ByteString str) {
		if (str == null) return null;
		return str.toStringUtf8();
	}

	public ByteString getBucketBS() {
		return bucket;
	}

	public String getBucket() {
		return bucket.toStringUtf8();
	}
	

	public ByteString getKeyBS() {
		return key;
	}
	
	public String getKey() {
		return key.toStringUtf8();
	}
	
	public ByteString getVclock() {
		return vclock;
	}

    public ByteString getValue(){
        return value;
    }

	RpbContent buildContent() {
		Builder b =
			RpbContent.newBuilder()
				.setValue(value);
		
		if (contentType != null) {
			b.setContentType(ByteString.copyFromUtf8(contentType));
		}
				
		if (charset != null) {
			b.setCharset(ByteString.copyFromUtf8(charset));
		}
				
		if (contentEncoding != null) {
			b.setContentEncoding(ByteString.copyFromUtf8(contentEncoding));
		}

		if (vtag != null) {
			b.setVtag(ByteString.copyFromUtf8(vtag));
		}
		
        if (links.size() != 0) {
            RiakLink[] localLinks = null;
            synchronized (links) {
                localLinks = links.toArray(new RiakLink[links.size()]);
            }
            for (RiakLink l : localLinks) {
                b.addLinks(l.build());
            }
        }
		
		if (lastModified != null) {
			b.setLastMod(lastModified);
		}
		
		if (lastModifiedUsec != null) {
			b.setLastModUsecs(lastModifiedUsec);
		}
		
		final Map<String, String> tmpUserMetaData = new LinkedHashMap<String, String>();

		synchronized (userMetaDataLock) {
		    tmpUserMetaData.putAll(userMetaData);
        }

		if (tmpUserMetaData != null && !tmpUserMetaData.isEmpty()) {
			for (Map.Entry<String, String> ent : tmpUserMetaData.entrySet()) {
				ByteString key = ByteString.copyFromUtf8(ent.getKey());
				com.basho.riak.pbc.RPB.RpbPair.Builder pb = RPB.RpbPair.newBuilder().setKey(key);
				if (ent.getValue() != null) {
					pb.setValue(ByteString.copyFromUtf8(ent.getValue()));
				}
				b.addUsermeta(pb);
			}
		}
		
		return b.build();
	}

	public String getVtag() {
	    return this.vtag;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

    public void addLink(String tag, String bucket, String key) {
        links.add(new RiakLink(bucket, key, tag));
    }

    public void addLink(ByteString tag, ByteString bucket, ByteString key) {
        links.add(new RiakLink(bucket, key, tag));
    }
    
    public List<RiakLink> getLinks() {
       return links != null ? Collections.unmodifiableList(links) : Collections.EMPTY_LIST;
    }

    /**
     * Add an item to the user meta data for this RiakObject.
     * @param key the key of the user meta data item
     * @param value the user meta data item
     * @return this RiakObject
     */
    public RiakObject addUsermetaItem(String key, String value) {
        synchronized (userMetaDataLock) {
            userMetaData.put(key, value);
        }
        return this;
    }

    /**
     * @return the lastModified
     */
    public Date getLastModified() {
        Date d = null;

        if (lastModified != null && lastModifiedUsec != null) {
            long t = (lastModified * 1000L ) + (lastModifiedUsec / 100L);
            d = new Date(t);
        }
        return d;
    }

	public String getContentType() {
		return contentType;
	}

}
