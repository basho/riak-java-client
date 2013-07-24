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
package com.basho.riak.pbc;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.protobuf.RiakKvPB.RpbCounterGetReq;
import com.basho.riak.protobuf.RiakKvPB.RpbGetReq;
import com.google.protobuf.ByteString;

/**
 * Encapsulate the set of request parameters for a fetch operation on the pb
 * interface
 * 
 * @author russell
 * 
 */
public class FetchMeta {

    private static final FetchMeta EMPTY = new FetchMeta(null, null, null, null, null, null, null, null);

    private final Integer r;
    private final Integer pr;
    private final Boolean notFoundOK;
    private final Boolean basicQuorum;
    private final Boolean headOnly;
    private final Boolean returnDeletedVClock;
    private final VClock ifModifiedVClock;
    private final Integer timeout; 

    
//    public FetchMeta(Integer r, Integer pr, Boolean notFoundOK, Boolean basicQuorum, Boolean headOnly,
//            Boolean returnDeletedVClock, VClock ifModifiedVClock) {
//        this(r, pr, notFoundOK, basicQuorum, headOnly, returnDeletedVClock, ifModifiedVClock, null );
//    }
    
    /**
     * @param r
     * @param pr
     * @param notFoundOK
     * @param basicQuorum
     * @param headOnly
     * @param returnDeletedVClock
     * @param timeout 
     * @param vtag
     *            if not null then a conditional fetch
     */
    public FetchMeta(Integer r, Integer pr, Boolean notFoundOK, Boolean basicQuorum, Boolean headOnly,
            Boolean returnDeletedVClock, VClock ifModifiedVClock, Integer timeout) {
        this.r = r;
        this.pr = pr;
        this.notFoundOK = notFoundOK;
        this.basicQuorum = basicQuorum;
        this.headOnly = headOnly;
        this.returnDeletedVClock = returnDeletedVClock;
        this.ifModifiedVClock = ifModifiedVClock;
        this.timeout = timeout;
    }

    public void write(RpbGetReq.Builder b) {
        if (r != null) {
            b.setR(r);
        }

        if (pr != null) {
            b.setPr(pr);
        }

        if (notFoundOK != null) {
            b.setNotfoundOk(notFoundOK);
        }

        if (basicQuorum != null) {
            b.setBasicQuorum(basicQuorum);
        }

        if (headOnly != null) {
            b.setHead(headOnly);
        }

        if (returnDeletedVClock != null) {
            b.setDeletedvclock(returnDeletedVClock);
        }

        if (ifModifiedVClock != null) {
            b.setIfModified(ByteString.copyFrom(ifModifiedVClock.getBytes()));
        }
        
        if (timeout != null) {
            b.setTimeout(timeout);
        }
    }

    public void writeCounter(RpbCounterGetReq.Builder b) {
        if (r != null) {
            b.setR(r);
        }

        if (pr != null) {
            b.setPr(pr);
        }

        if (notFoundOK != null) {
            b.setNotfoundOk(notFoundOK);
        }

        if (basicQuorum != null) {
            b.setBasicQuorum(basicQuorum);
        }
    }
    
    /**
     * @return an empty fetch meta
     */
    public static com.basho.riak.pbc.FetchMeta empty() {
        return EMPTY;
    }
}
