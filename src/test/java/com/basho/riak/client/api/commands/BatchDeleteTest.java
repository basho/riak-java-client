package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.BatchDelete;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.operations.DeleteOperation;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

public class BatchDeleteTest {

    @Mock
    RiakCluster mockCluster;
    @Mock
    RiakFuture mockFuture;
    Location key1 = new Location(new Namespace("type1", "bucket1"), "key1");
    Location key2 = new Location(new Namespace("type2", "bucket2"), "key2");
    RiakClient client;

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        Mockito.when(mockFuture.get()).thenReturn(null);
        Mockito.when(mockFuture.get(Matchers.anyLong(), Matchers.any(TimeUnit.class))).thenReturn(null);
        Mockito.when(mockFuture.isCancelled()).thenReturn(false);
        Mockito.when(mockFuture.isDone()).thenReturn(true);
        Mockito.when(mockFuture.isSuccess()).thenReturn(true);
        Mockito.when(mockCluster.<DeleteOperation, Location>execute(Matchers.any(FutureOperation.class))).thenReturn(mockFuture);
        client = new RiakClient(mockCluster);
    }

    @Test
    public void testExecuteAsync() throws Exception {
        BatchDelete.Builder batchDeleteBuilder = new BatchDelete.Builder();
        batchDeleteBuilder
                .withTimeout(3000)
                .addLocations(key1, key2);
        BatchDelete batchDelete = batchDeleteBuilder.build();
        client.executeAsync(batchDelete);

    }
}
