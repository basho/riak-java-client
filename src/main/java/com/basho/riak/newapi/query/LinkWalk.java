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

import java.io.IOException;
import java.util.LinkedList;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.newapi.RiakException;
import com.basho.riak.newapi.IRiakObject;
import com.basho.riak.newapi.operations.RiakOperation;
import com.basho.riak.newapi.query.LinkWalkStep.Accumulate;

/**
 * 
 * @author russell
 * 
 */
public class LinkWalk implements RiakOperation<WalkResult> {

    private final RawClient client;
    private final String startBucket;
    private final String startKey;
    private final LinkedList<LinkWalkStep> steps = new LinkedList<LinkWalkStep>();

    /**
     * @param startObject
     */
    public LinkWalk(final RawClient client, final IRiakObject startObject) {
        this.client = client;
        this.startBucket = startObject.getBucket();
        this.startKey = startObject.getKey();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.basho.riak.client.RiakOperation#execute()
     */
    public WalkResult execute() throws RiakException {
        try {
            return client.linkWalk(new LinkWalkSpec(steps, startBucket, startKey));
        } catch (IOException e) {
            throw new RiakException(e);
        }
    }

    /**
     * Add a link walking step to this link walk
     *
     * @param bucket
     *            the bucket, a null, or empty string is treated as the wildcard
     * @param tag
     *            the tag of the link, a null or empty string is treated as the
     *            wildcard
     * @param accumulate
     *            to keep the result of this step or not
     * @return this
     */
    public LinkWalk addStep(String bucket, String tag, Accumulate accumulate) {
        synchronized (steps) {
            steps.add(new LinkWalkStep(bucket, tag, accumulate));
        }
        return this;
    }

    /**
     * Add a link walking step to this link walk
     *
     * @param bucket
     *            the bucket, a null, or empty string is treated as the wildcard _
     * @param tag
     *            the tag of the link, a null or empty string is treated as the
     *            wildcard
     * @param accumulate
     *            to keep the result of this step or not
     * @return this
     */
    public LinkWalk addStep(String bucket, String tag, boolean keep) {
        synchronized (steps) {
            steps.add(new LinkWalkStep(bucket, tag, keep));
        }
        return this;
    }

    /**
     * Add a link walking step to this link walk using the default accumulate
     * value for the step (NO for all steps accept last step)
     *
     * @param bucket
     *            the bucket, a null, or empty string is treated as the wildcard _
     * @param tag
     *            the tag of the link, a null or empty string is treated as the
     *            wildcard
     * @return this
     */
    public LinkWalk addStep(String bucket, String tag) {
        synchronized (steps) {
            steps.add(new LinkWalkStep(bucket, tag));
        }
        return this;
    }
}
