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

import com.basho.riak.pbc.RPB.RpbDelReq.Builder;
import com.google.protobuf.ByteString;

/**
 * Encapsulate the set of parameters for a delete operation.
 * 
 * @author russell
 * 
 */
public class DeleteMeta {

    private final Integer r;
    private final Integer pr;
    private final Integer w;
    private final Integer dw;
    private final Integer pw;
    private final Integer rw;
    private final byte[] vclock;

    /**
     * @param r
     * @param pr
     * @param w
     * @param dw
     * @param pw
     * @param rw
     * @param vclock
     */
    public DeleteMeta(Integer r, Integer pr, Integer w, Integer dw, Integer pw, Integer rw, byte[] vclock) {
        this.r = r;
        this.pr = pr;
        this.w = w;
        this.dw = dw;
        this.pw = pw;
        this.rw = rw;
        this.vclock = vclock == null ? null : vclock.clone();
    }

    /**
     * @param builder
     */
    public void write(Builder builder) {
        if (r != null) {
            builder.setR(r);
        }

        if (pr != null) {
            builder.setPr(pr);
        }

        if (w != null) {
            builder.setW(w);
        }

        if (dw != null) {
            builder.setDw(dw);
        }

        if (pw != null) {
            builder.setPw(pw);
        }

        if (rw != null) {
            builder.setRw(rw);
        }

        if (vclock != null) {
            builder.setVclock(ByteString.copyFrom(vclock));
        }
    }

    /**
     * @return an empty DeleteMeta
     */
    public static com.basho.riak.pbc.DeleteMeta empty() {
        return new DeleteMeta(null, null, null, null, null, null, null);
    }
}
