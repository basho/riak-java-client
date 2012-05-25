package com.basho.riak.client.raw.cluster;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.basho.riak.client.raw.RawClient;

import static org.mockito.Mockito.mock;

public class ClientResolverTest {

    @Test public void getNextClient() {
        ClientResolver clientResolver = new ClientResolver();
        List<RawClient> clients = Arrays.asList(mock(RawClient.class), mock(RawClient.class), mock(RawClient.class));
        assert clients.get(0) == clientResolver.getNextClient(clients);
        assert clients.get(1) == clientResolver.getNextClient(clients);
        assert clients.get(2) == clientResolver.getNextClient(clients);
        assert clients.get(0) == clientResolver.getNextClient(clients);
        clients = Arrays.asList(mock(RawClient.class));
        assert clients.get(0) == clientResolver.getNextClient(clients);
        assert clients.get(0) == clientResolver.getNextClient(clients);
    }
}
