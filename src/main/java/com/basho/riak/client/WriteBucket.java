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
package com.basho.riak.client;

/**
 * @author russell
 * 
 */
public class WriteBucket implements RiakOperation<Bucket> {

    private Bucket bucket;
    private String name;

    private Quorum r;
    private Quorum w;
    private Quorum dw;
    private Quorum rw;
    private Integer nval;
    private Boolean allowSiblings;
    private NamedErlangFunction chashKeyFunction;
    private int retries = 0;

    public WriteBucket(Bucket b) {
        this.bucket = b;
    }

    public WriteBucket(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public Bucket execute() throws RiakException {
        // TODO Auto-generated method stub
        return null;
    }

    public WriteBucket r(CAP quorum) {
        this.r = new Quorum(quorum);
        return this;
    }

    public WriteBucket r(int quorum) {
        this.r = new Quorum(quorum);
        return this;
    }

    public WriteBucket w(CAP quorum) {
        this.w = new Quorum(quorum);
        return this;
    }

    public WriteBucket w(int quorum) {
        this.w = new Quorum(quorum);
        return this;
    }

    public WriteBucket dw(CAP quorum) {
        this.dw = new Quorum(quorum);
        return this;
    }

    public WriteBucket dw(int quorum) {
        this.dw = new Quorum(quorum);
        return this;
    }

    public WriteBucket rw(CAP quorum) {
        this.rw = new Quorum(quorum);
        return this;
    }

    public WriteBucket rw(int quorum) {
        this.rw = new Quorum(quorum);
        return this;
    }

    public WriteBucket nval(int nval) {
        this.nval = nval;
        return this;
    }

    public WriteBucket allowSiblings(boolean allowSiblings) {
        this.allowSiblings = allowSiblings;
        return this;
    }

    /**
     * @param times
     * @return
     */
    public WriteBucket retry(int times) {
        this.retries = times;
        return this;
    }

    /**
     * @param namedErlangFunction
     * @return
     */
    public WriteBucket chashKeyFunction(NamedErlangFunction chashKeyFunction) {
        this.chashKeyFunction = chashKeyFunction;
        return this;
    }

    private static final class Quorum {
        private Integer i;
        private CAP cap;

        public Quorum(int i) {
            this.i = i;
        }

        public Quorum(CAP cap) {
            this.cap = cap;
        }

        boolean isCAP() {
            return cap != null;
        }

        boolean isInt() {
            return i != null;
        }
    }
}
