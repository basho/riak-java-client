package com.basho.riak.client.core;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakCommand;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 */
public class RiakClientTest
{
    private static RiakClient client = new RiakClient(mock(RiakCluster.class));

    @Test
    public void clientExecutesCommand() throws UnknownHostException, ExecutionException, InterruptedException
    {
        final CommandFake command = new CommandFake();

        client.execute(command);
        assertTrue(command.executeAsyncCalled);
    }

    @Test
    public void clientExecutesTimeoutCommand()
            throws UnknownHostException, ExecutionException, InterruptedException, TimeoutException
    {
        final CommandFake command = new CommandFake();

        client.execute(command, 1, TimeUnit.SECONDS);
        assertTrue(command.executeAsyncCalled);
        verify(command.getMockFuture()).get(1, TimeUnit.SECONDS);
    }

    public class CommandFake extends RiakCommand<String, String>
    {
        private boolean executeAsyncCalled = false;
        private RiakFuture<String, String> mockFuture = (RiakFuture<String, String>) mock(RiakFuture.class);

        @Override
        protected RiakFuture<String, String> executeAsync(RiakCluster cluster)
        {
            executeAsyncCalled = true;
            return mockFuture;
        }

        public boolean wasExecuteAsyncCalled()
        {
            return executeAsyncCalled;
        }

        public RiakFuture<String, String> getMockFuture()
        {
            return mockFuture;
        }
    }
}
