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
package com.basho.riak.client.api.commands.mapreduce;

/**
 * {@link MapReducePhase} implementation that models a Link Phase
 *
 * @author russell
 * @see MapReduce
 * @see MapPhase
 * @see ReducePhase
 */
class LinkPhase extends MapReducePhase
{

    private final String bucket;
    private final String tag;
    private final Boolean keep;

    /**
     * Create a Link Phase that points to <code>bucket</code> / <code>tag</code>
     * .
     *
     * @param bucket the bucket at the end of the link (or "_" or "" for wildcard)
     * @param tag    the tag (or ("_", or "" for wildcard)
     * @param keep   to keep the result of this phase and return it at the end of
     *               the operation
     */
    public LinkPhase(String bucket, String tag, Boolean keep)
    {
        super(PhaseType.LINK);
        this.bucket = bucket;
        this.tag = tag;
        this.keep = keep;
    }

    /**
     * Create a Link Phase that points to <code>bucket</code> / <code>tag</code>
     * <code>keep</code> will be <code>false</code>
     *
     * @param bucket the bucket at the end of the link (or "_" or "" for wildcard)
     * @param tag    the tag (or ("_", or "" for wildcard)
     */
    public LinkPhase(String bucket, String tag)
    {
        this(bucket, tag, null);
    }

    /**
     * The bucket for this link phase
     *
     * @return the bucket
     */
    public String getBucket()
    {
        return bucket;
    }

    /**
     * The tag for this link phase
     *
     * @return the tag
     */
    public String getTag()
    {
        return tag;
    }

    /**
     * Keep the result or just use as input to the next phase?
     *
     * @return whether the result is kept or just passed to the next phase.
     */
    public Boolean isKeep()
    {
        return keep;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.newapi.query.MapReducePhase#getType()
     */
    public PhaseType getType()
    {
        return PhaseType.LINK;
    }
}
