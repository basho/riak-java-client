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
package com.basho.riak.client.raw.query.indexes;

import java.io.IOException;

import com.basho.riak.client.query.indexes.IntIndex;

/**
 * Concrete implementation of a range query for an _int index
 * 
 * @author russell
 * 
 */
public final class IntRangeQuery extends AbstractRangeQuery<Long> {

    /**
     * Create a range query
     * 
     * @param index
     *            the {@link IntIndex} to query
     * @param bucket
     *            the bucket to query
     * @param from
     *            the start of the range
     * @param to
     *            the end of the range
     */
    public IntRangeQuery(IntIndex index, String bucket, Long from, Long to) {
        super(index.getFullname(), bucket, from, to);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.raw.query.indexes.IndexQuery#write(com.basho.riak
     * .client.raw.query.indexes.IndexWriter)
     */
    public void write(IndexWriter writer) throws IOException {
        writer.write(getBucket(), getIndex(), from(), to());
    }
}
