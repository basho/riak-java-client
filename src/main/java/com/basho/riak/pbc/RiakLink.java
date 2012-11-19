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
import java.util.List;

import com.basho.riak.protobuf.RiakKvPB.RpbLink;
import com.basho.riak.protobuf.RiakKvPB.RpbLink.Builder;
import com.google.protobuf.ByteString;

/**
 * PBC specific model of a Riak Link.
 */
public class RiakLink {

	private final ByteString bucket;
	private final ByteString key;
	private final ByteString tag;

	RiakLink(RpbLink rpbLink) {
		this.bucket = rpbLink.getBucket();
		this.key = rpbLink.getKey();
		this.tag = rpbLink.getTag();
	}

	public RiakLink(String bucket, String key, String tag) {
		this.bucket = ByteString.copyFromUtf8(bucket);
		this.key = ByteString.copyFromUtf8(key);
		this.tag = ByteString.copyFromUtf8(tag);
	}

	public RiakLink(ByteString bucket, ByteString key, ByteString tag) {
		this.bucket = (bucket);
		this.key = (key);
		this.tag = (tag);
	}

	public static List<RiakLink> decode(List<RpbLink> list) {
		List<RiakLink>  res = new ArrayList<RiakLink>();
		
		for (RpbLink o : list) {
			res.add(new RiakLink(o));
		}
		
		return res;
	}

  public ByteString getBucket() {
    return bucket;
  }

  public ByteString getKey() {
    return key;
  }

  public ByteString getTag() {
    return tag;
  }

	public RpbLink build() {
		Builder b = RpbLink.newBuilder();
		
		if (bucket != null)
			b.setBucket(bucket);
		
		if (key != null)
			b.setKey(key);
		
		if (tag != null) 
			b.setTag(tag);

		return b.build();
	}

}
