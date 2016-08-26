package com.basho.riak.client.api.commands;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.MultiDelete;
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
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;

public class MultiDeleteTest
{
    @Mock
    RiakCluster mockCluster;
    @Mock
    RiakFuture mockFuture;
    Location key1 = new Location(new Namespace("type1", "bucket1"), "key1");
    Location key2 = new Location(new Namespace("type2", "bucket2"), "key2");
    RiakClient client;

    @Before
    @SuppressWarnings("unchecked")
    public void init() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        when(mockFuture.get()).thenReturn(null);
        when(mockFuture.get(Matchers.anyLong(), Matchers.any(TimeUnit.class))).thenReturn(null);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);
        when(mockFuture.isSuccess()).thenReturn(true);
        when(mockCluster.<DeleteOperation, Location>execute(Matchers.any(FutureOperation.class)))
                .thenReturn(mockFuture);
        client = new RiakClient(mockCluster);
    }

    @Test
    public void testExecuteAsync() throws Exception
    {
        MultiDelete.Builder multiDeleteBuilder = new MultiDelete.Builder();
        multiDeleteBuilder.withTimeout(3000).addLocations(key1, key2);
        MultiDelete multiDelete = multiDeleteBuilder.build();
        client.executeAsync(multiDelete);
    }
}
