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
package com.basho.riak.client.query;

/**
 * Used internally by {@link LinkWalk} to model a step in a link walk operation.
 * 
 * @author russell
 * 
 * @see LinkWalk#addStep(String, String, Accumulate)
 * @see LinkWalk#addStep(String, String)
 * @see LinkWalk#addStep(String, String, boolean)
 */
public class LinkWalkStep {

    /**
     * Enum for the accumulate specification of a link walk step.
     * 
     * <p>
     * By default a link walk step does not accumulate (IE return data) it only
     * provides input to the next step. However the *final* step in a Link walk
     * , by default, will return the results.
     * </p>
     */
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

    /**
     * Create a step definition.
     * 
     * @param bucket
     *            the bucket or null/"","_" for a wildcard (meaning *any*)
     * @param tag
     *            the tag or null/"","_" for a wildcard (meaning *any*)
     * @param keep
     *            yes, no, default?
     */
    public LinkWalkStep(String bucket, String tag, Accumulate keep) {
        this.bucket = bucket;
        this.tag = tag;
        this.keep = keep;
    }

    /**
     * Create a step definition.
     * 
     * @param bucket
     *            the bucket or null/"","_" for a wildcard (meaning *any*)
     * @param tag
     *            the tag or null/"","_" for a wildcard (meaning *any*)
     * @param keep
     *            true/false?
     */
    public LinkWalkStep(String bucket, String tag, boolean keep) {
        this.bucket = bucket;
        this.tag = tag;
        this.keep = Accumulate.fromBoolean(keep);
    }

    /**
     * Create a step definition with default value for accumulate
     * 
     * @param bucket
     *            the bucket or null/"","_" for a wildcard (meaning *any*)
     * @param tag
     *            the tag or null/"","_" for a wildcard (meaning *any*)
     */
    public LinkWalkStep(String bucket, String tag) {
        this.bucket = bucket;
        this.tag = tag;
        this.keep = Accumulate.DEFAULT;
    }

    /**
     * @return the bucket
     */
    public String getBucket() {
        return bucket;
    }

    /**
     * @return the tag
     */
    public String getTag() {
        return tag;
    }

    /**
     * @return the value for keep/accumulate
     */
    public Accumulate getKeep() {
        return keep;
    }

}
