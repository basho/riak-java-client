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

import com.basho.riak.client.cap.VClock;

/**
 * The set of parameters for a delete operation
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
    private final VClock vclock;

    /**
     * Any of the parameters may be null.
     * 
     * @param r
     * @param pr
     * @param w
     * @param dw
     * @param pw
     * @param rw
     * @param vclock
     */
    public DeleteMeta(Integer r, Integer pr, Integer w, Integer dw, Integer pw, Integer rw, VClock vclock) {
        this.r = r;
        this.pr = pr;
        this.w = w;
        this.dw = dw;
        this.pw = pw;
        this.rw = rw;
        this.vclock = vclock;
    }

    /**
     * @return true is the r parameter is set, false otherwise.
     */
    public boolean hasR() {
        return r != null;
    }

    /**
     * @return r parameter or null
     */
    public Integer getR() {
        return r;
    }

    /**
     * @return true if the pr parameter is set, false otherwise
     */
    public boolean hasPr() {
        return pr != null;
    }

    /**
     * @return the pr parameter, or null
     */
    public Integer getPr() {
        return pr;
    }

    /**
     * @return true if the w parameter is set, false otherwise
     */
    public boolean hasW() {
        return w != null;
    }

    /**
     * @return the w parameter or null
     */
    public Integer getW() {
        return w;
    }

    /**
     * @return true if the dw parameter is set, false otherwise
     */
    public boolean hasDw() {
        return dw != null;
    }

    /**
     * @return the dw paramter, or null
     */
    public Integer getDw() {
        return dw;
    }

    /**
     * @return true is the pw parameter is set, false otherwise.
     */
    public boolean hasPw() {
        return pw != null;
    }

    /**
     * @return pw parameter, or null
     */
    public Integer getPw() {
        return pw;
    }

    /**
     * @return true if the rw parameter is set
     */
    public boolean hasRw() {
        return rw != null;
    }

    /**
     * @return the rw, or null if not set.
     */
    public Integer getRw() {
        return rw;
    }

    /**
     * @return true if this delete meta has a vclock
     */
    public boolean hasVclock() {
        return vclock != null;
    }

    /**
     * @return the vclock or null if not set
     */
    public VClock getVclock() {
        return vclock;
    }

}
