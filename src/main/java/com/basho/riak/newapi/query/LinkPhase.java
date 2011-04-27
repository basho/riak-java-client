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
public class LinkPhase implements MapReducePhase {

    private final String bucket;
    private final String tag;
    private final boolean keep;

    /**
     * @param bucket
     * @param tag
     * @param keep
     */
    public LinkPhase(String bucket, String tag, boolean keep) {
        this.bucket = bucket;
        this.tag = tag;
        this.keep = keep;
    }

    /**
     * @param bucket
     * @param tag
     */
    public LinkPhase(String bucket, String tag) {
        this.bucket = bucket;
        this.tag = tag;
        this.keep = false;
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
     * @return whether the result is kept or just passed to the next phase.
     */
    public boolean isKeep() {
        return keep;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.newapi.query.MapReducePhase#getType()
     */
    public PhaseType getType() {
        return PhaseType.LINK;
    }
}
