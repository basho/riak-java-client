/*
 * Copyright 2013-2015 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations.ts;

import java.util.Iterator;
import java.util.List;

import com.basho.riak.client.api.commands.timeseries.CoveragePlan;
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response;
import com.basho.riak.client.core.operations.PBFutureOperation;
import com.basho.riak.client.core.query.timeseries.CoveragePlanResult;
import com.basho.riak.client.core.query.timeseries.PbResultFactory;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.google.protobuf.ByteString;

/**
 * An operation to retrieve a coverage plan from RiakTS.
 *
 */

public class CoveragePlanOperation extends PBFutureOperation<CoveragePlanResult, RiakTsPB.TsCoverageResp, String> {
    private final String tableName;
    private final String queryText;

    private CoveragePlanOperation(AbstractBuilder builder) {
        super(RiakMessageCodes.MSG_TsCoverageReq, 
                RiakMessageCodes.MSG_TsCoverageResp, 
                builder.reqBuilder,
                RiakTsPB.TsCoverageResp.PARSER);

        this.queryText = builder.queryText;
        this.tableName = builder.tableName;
    }

    @Override
    protected CoveragePlanResult convert(List<RiakTsPB.TsCoverageResp> responses) {
        // This is not a streaming op, there will only be one response
        final RiakTsPB.TsCoverageResp response = checkAndGetSingleResponse(responses);
        return PbResultFactory.convertCoverageResp(this.tableName, response);
    }

    @Override
    public String getQueryInfo() {
        return this.queryText;
    }

    public static abstract class AbstractBuilder<R> {
        private final String tableName;
        private final String queryText;
        private final RiakTsPB.TsCoverageReq.Builder reqBuilder = RiakTsPB.TsCoverageReq.newBuilder();

        public AbstractBuilder(String tableName, String queryText) {
            if (tableName == null || tableName.length() == 0) {
                throw new IllegalArgumentException("Table Name cannot be null or empty");
            }

            if (queryText == null || queryText.length() == 0) {
                throw new IllegalArgumentException("Query cannot be null or empty");
            }

            reqBuilder.setTable(ByteString.copyFromUtf8(tableName));
            reqBuilder.setQuery(RiakTsPB.TsInterpolation.newBuilder().setBase(ByteString.copyFromUtf8(queryText)));
            this.tableName = tableName;
            this.queryText = queryText;
        }

        public AbstractBuilder<R> withReplaceCoverageEntry(Response.CoverageEntry coverageEntry) {
            return withReplaceCoverageContext(coverageEntry.getCoverageContext());
        }

        public AbstractBuilder<R> withReplaceCoverageContext(byte[] coverageContext) {
            reqBuilder.setReplaceCover(ByteString.copyFrom(coverageContext));
            return this;
        }

        public AbstractBuilder<R> withUnavailableCoverageContext(Iterable<byte[]> coverageContext) {
            for (Iterator<byte[]> iterator = coverageContext.iterator(); iterator.hasNext();) {
                withUnavailableCoverageContext(iterator.next());
            }
            return this;
        }

        public AbstractBuilder<R> withUnavailableCoverageEntries(Iterable<Response.CoverageEntry> coverageEntries) {
            for (Response.CoverageEntry coverageEntry : coverageEntries) {
                withUnavailableCoverageContext(coverageEntry.getCoverageContext());
            }
            return this;
        }

        public AbstractBuilder<R> withUnavailableCoverageContext(byte[]... coverageContext) {
            for (byte[] cc : coverageContext) {
                reqBuilder.addUnavailableCover(ByteString.copyFrom(cc));
            }
            return this;
        }

        public CoveragePlanOperation buildOperation(){
            return new CoveragePlanOperation(this);
        }

        public abstract R build();
    }
}
