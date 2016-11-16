package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.*;
import com.basho.riak.client.core.operations.ListBucketsOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author empovit
 * @since 2.0.3
 */
public class ListBucketsTest
{
    @Mock RiakCluster mockCluster;
    @Mock StreamingRiakFuture mockFuture;
    @Mock ListBucketsOperation.Response mockResponse;
    RiakClient client;

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        when(mockResponse.getBuckets()).thenReturn(new ArrayList<>());
        when(mockFuture.get()).thenReturn(mockResponse);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(mockResponse);
        when(mockFuture.getNow()).thenReturn(mockResponse);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);
        when(mockFuture.isSuccess()).thenReturn(true);
        doReturn(mockFuture).when(mockCluster).<ListBucketsOperation,Location>execute(any(FutureOperation.class));
        client = new RiakClient(mockCluster);
    }

    @SuppressWarnings("unchecked")
    private void testListBuckets(String bucketType) throws Exception
    {
        final BinaryValue type = BinaryValue.createFromUtf8(bucketType);
        ListBuckets.Builder list = new ListBuckets.Builder(type);
        client.execute(list.build());

        ArgumentCaptor<FutureOperation> captor =
                ArgumentCaptor.forClass(FutureOperation.class);
        verify(mockCluster).execute(captor.capture());

        ListBucketsOperation operation = (ListBucketsOperation)captor.getValue();
        RiakKvPB.RpbListBucketsReq.Builder builder =
                (RiakKvPB.RpbListBucketsReq.Builder) Whitebox.getInternalState(operation, "reqBuilder");

        Assert.assertEquals(ByteString.copyFrom(type.unsafeGetValue()), builder.getType());
    }

    @Test
    public void bucketTypeBuiltCorrectly() throws Exception
    {
        testListBuckets("bucket_type");
    }

    @Test
    public void defaultBucketTypeBuiltCorrectly() throws Exception
    {
        testListBuckets(Namespace.DEFAULT_BUCKET_TYPE);
    }
}
