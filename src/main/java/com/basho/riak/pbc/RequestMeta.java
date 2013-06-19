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

import com.google.protobuf.ByteString;

/**
 * Concrete implementation of the PBC request meta.
 */
public class RequestMeta implements IRequestMeta {

	Boolean returnBody;
	Integer writeQuorum;
	Integer durableWriteQuorum;
	String contentType;
	Integer pw;
	Boolean ifNotModified;
	Boolean ifNoneMatch;
	Boolean returnHead;
    Boolean asis;
	
	public RequestMeta() {
	}
	
	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#preparePut(com.trifork.riak.RPB.RpbPutReq.Builder)
	 */
	public void preparePut(com.basho.riak.protobuf.RiakKvPB.RpbPutReq.Builder builder) {

		if (returnBody != null) {
			builder.setReturnBody(returnBody.booleanValue());
		}
		
		if (writeQuorum != null) {
			builder.setW(writeQuorum.intValue());
		}
		
		if (durableWriteQuorum != null) {
			builder.setDw(durableWriteQuorum.intValue());
		}

        if (pw != null) {
            builder.setPw(pw);
        }

        if (ifNoneMatch != null) {
            builder.setIfNoneMatch(ifNoneMatch);
        }

        if (ifNotModified != null) {
            builder.setIfNotModified(ifNotModified);
        }

        if (returnHead != null) {
            builder.setReturnHead(returnHead.booleanValue());
        }
        
        if (asis != null) {
            builder.setAsis(asis.booleanValue());
        }
	}

	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#returnBody(boolean)
	 */
	public IRequestMeta returnBody(boolean ret) {
		returnBody = Boolean.valueOf(ret);
		return this;
	}

	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#w(int)
	 */
	public IRequestMeta w(int w) {
	    writeQuorum = new Integer(w);
		return this;
	}

	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#dw(int)
	 */
	public IRequestMeta dw(int dw) {
	    durableWriteQuorum = new Integer(dw);
		return this;
	}
	
	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#contentType(java.lang.String)
	 */
	public IRequestMeta contentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	/* (non-Javadoc)
	 * @see com.trifork.riak.IRequestMeta#getContentType()
	 */
	public ByteString getContentType() {
		return ByteString.copyFromUtf8(contentType);
	}

    public IRequestMeta pw(int pw) {
        this.pw = pw;
        return this;
    }

    public IRequestMeta ifNoneMatch(boolean ifNoneMatch) {
        this.ifNoneMatch = ifNoneMatch;
        return this;
    }

    public IRequestMeta ifNotModified(boolean ifNotModified) {
        this.ifNotModified = ifNotModified;
        return this;
    }

    public IRequestMeta returnHead(boolean returnHead) {
        this.returnHead = returnHead;
        return this;
    }
    
    public IRequestMeta asis(boolean asis) {
        this.asis = asis;
        return this;
    }
}
