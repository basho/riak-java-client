package com.basho.riak.client.core.query.timeseries;

import com.basho.riak.protobuf.RiakTsPB;
import com.basho.riak.protobuf.RiakTsPB.TsRange;

import java.util.Iterator;
import java.util.List;

import org.slf4j.LoggerFactory;

/**
 * @author Alex Moore <amoore at basho dot com>
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.3
 */
public final class PbResultFactory
{
    private PbResultFactory() {}

    public static QueryResult convertPbQueryResp(RiakTsPB.TsQueryResp response)
    {
        if(response == null)
        {
            return QueryResult.EMPTY;
        }

        return new QueryResult(response.getColumnsList(), response.getRowsList());
    }

    public static QueryResult convertPbGetResp(RiakTsPB.TsGetResp response)
    {
        if (response == null)
        {
            return QueryResult.EMPTY;
        }

        return new QueryResult(response.getColumnsList(), response.getRowsList());
    }

    public static QueryResult convertPbListKeysResp(List<RiakTsPB.TsListKeysResp> responseChunks)
    {
        if (responseChunks == null)
        {
            return QueryResult.EMPTY;
        }

        int totalKeyCount = 0;

        for (RiakTsPB.TsListKeysResp responseChunk : responseChunks)
        {
            totalKeyCount += responseChunk.getKeysCount();
        }

        FlatteningIterable<RiakTsPB.TsListKeysResp, RiakTsPB.TsRow> flatIterable =
                new FlatteningIterable<RiakTsPB.TsListKeysResp, RiakTsPB.TsRow>(responseChunks,
                    new FlatteningIterable.InnerIterableProvider<RiakTsPB.TsListKeysResp, RiakTsPB.TsRow>()
                    {
                        @Override
                        public Iterator<RiakTsPB.TsRow> getInnerIterator(RiakTsPB.TsListKeysResp provider)
                        {
                            return provider.getKeysList().iterator();
                        }
                    });

        return new QueryResult(flatIterable, totalKeyCount);
    }

    public static TableDefinition convertDescribeResp(String tableName, RiakTsPB.TsQueryResp response)
    {
        if (response == null || response.getRowsCount() == 0)
        {
            return null;
        }

        final QueryResult intermediaryQueryResult = new QueryResult(response.getColumnsList(), response.getRowsList());

        return new TableDefinition(
                tableName,
                CollectionConverters.convertDescribeQueryResultToColumnDescriptions(intermediaryQueryResult));

    }

    public static CoveragePlanResult convertCoverageResp(String tableName, RiakTsPB.TsCoverageResp response)
    {
        if (response == null || response.getEntriesCount() == 0)
        {
            return null;
        }
        CoveragePlanResult r = new CoveragePlanResult();
        for (RiakTsPB.TsCoverageEntry e : response.getEntriesList())
        {
            final CoverageEntry ce = new CoverageEntry();
            ce.setCoverageContext(e.getCoverContext().toByteArray());
            if (e.hasRange())
            {
                TsRange range = e.getRange();
                ce.setFieldName(range.getFieldName().toStringUtf8());
                ce.setLowerBound(range.getLowerBound());
                ce.setLowerBoundInclusive(range.getLowerBoundInclusive());
                ce.setUpperBound(range.getUpperBound());
                ce.setUpperBoundInclusive(range.getUpperBoundInclusive());
                ce.setDescription(range.getDesc().toStringUtf8());
            }
            ce.setHost(e.getIp().toStringUtf8());
            ce.setPort(e.getPort());

            if ("0.0.0.0".equals(ce.getHost()))
            {
                LoggerFactory.getLogger(CoveragePlanResult.class).error(
                        "CoveragePlanOperation returns at least one coverage entry: '{}' -- with IP address '0.0.0.0'.\n"
                                + "Execution will be failed due to the imposibility of using IP '0.0.0.0' "
                                + "for querying data from the remote Riak.", ce);

                throw new RuntimeException("CoveragePlanOperation returns at least one coverage entry with ip '0.0.0.0'.");
            }

            r.addEntry(ce);
        }
        return r;
    }
}
