package com.basho.riak.client.core.operations;

import com.basho.riak.client.api.commands.kv.CoveragePlan;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

/**
 * @author Sergey Galkin <sgalkin at basho dot com>
 */
public class CoveragePlanOperationTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void raiseExceptionDuringConversionInCaseWhenCoverageEntryHasZeroIp()
    {
        final Namespace ns = new Namespace(Namespace.DEFAULT_BUCKET_TYPE, "bucket");
        final CoveragePlan.Builder builder = new CoveragePlan.Builder(ns);
        final CoveragePlanOperation op = builder.buildOperation();

        final RiakKvPB.RpbCoverageEntry e = RiakKvPB.RpbCoverageEntry.newBuilder()
                .setCoverContext(ByteString.copyFromUtf8("some-context"))
                .setIp(ByteString.copyFromUtf8("0.0.0.0"))
                .setPort(1111)
                .setKeyspaceDesc(ByteString.copyFromUtf8("entry with Zero ip"))
                .build();

        final RiakKvPB.RpbCoverageResp rpbCoverageResp = RiakKvPB.RpbCoverageResp.newBuilder()
                .addEntries(e)
                .build();

        exception.expect(RuntimeException.class);
        exception.expectMessage("CoveragePlanOperation returns at least one coverage entry with ip '0.0.0.0'.");

        op.convert(Collections.singletonList(rpbCoverageResp));

    }
}
