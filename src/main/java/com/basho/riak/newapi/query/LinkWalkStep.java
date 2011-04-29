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
package com.basho.riak.newapi.query;

/**
 * @author russell
 * 
 */
public class LinkWalkStep {

    public enum Accumulate {
        YES("1"), NO("2"), DEFAULT("_");

        private final String asString;

        private Accumulate(String asString) {
            this.asString = asString;
        }

        public String toString() {
            return asString;
        }

        public static Accumulate fromBoolean(boolean bool) {
            if (bool) {
                return Accumulate.YES;
            } else {
                return Accumulate.NO;
            }
        }
    };

    private final String bucket;
    private final String tag;
    private final Accumulate keep;

    public LinkWalkStep(String bucket, String key, Accumulate keep) {
        this.bucket = bucket;
        this.tag = key;
        this.keep = keep;
    }

    public LinkWalkStep(String bucket, String key, boolean keep) {
        this.bucket = bucket;
        this.tag = key;
        this.keep = Accumulate.fromBoolean(keep);
    }

    public LinkWalkStep(String bucket, String key) {
        this.bucket = bucket;
        this.tag = key;
        this.keep = Accumulate.DEFAULT;
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @return the key
     */
    public String getKey() {
        return tag;
    }

    /**
     * @return the keep
     */
    public Accumulate getKeep() {
        return keep;
    }

}
