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

package com.trifork.riak;

import com.google.protobuf.ByteString;

public class RequestMeta {

	Boolean returnBody;
	Integer writeQuorum;
	Integer durableWriteQuorum;
	String contentType;
	
	public RequestMeta() {
	}
	
	public void preparePut(RPB.RpbPutReq.Builder builder) {

		if (returnBody != null) {
			builder.setReturnBody(returnBody.booleanValue());
		}
		
		if (writeQuorum != null) {
			builder.setW(writeQuorum.intValue());
		}
		
		if (durableWriteQuorum != null) {
			builder.setDw(durableWriteQuorum.intValue());
		}		
	}

	public RequestMeta returnBody(boolean ret) {
		returnBody = Boolean.valueOf(ret);
		return this;
	}

	public RequestMeta w(int w) {
		w = new Integer(w);
		return this;
	}

	public RequestMeta dw(int dw) {
		dw = new Integer(dw);
		return this;
	}
	
	public RequestMeta contentType(String contentType) {
		this.contentType = contentType;
		return this;
	}

	public ByteString getContentType() {
		return ByteString.copyFromUtf8(contentType);
	}
}
