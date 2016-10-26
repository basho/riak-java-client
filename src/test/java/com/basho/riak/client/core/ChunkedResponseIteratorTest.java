package com.basho.riak.client.core;


import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * @author Alex Moore <amoore at basho dot com>
 */
public class ChunkedResponseIteratorTest
{
    /*
      NB: Disable this test if you want to use code coverage tools.
      The Thread interrupt it generates triggers a shutdown hook race bug in Java,
      which then doesn't allow EMMA to cleanly shutdown.
      https://bugs.openjdk.java.net/browse/JDK-8154017
     */
    @Test
    public void testInterruptedExceptionDealtWith() throws InterruptedException
    {
        final boolean[] caught = {false};
        final InterruptedException[] ie = {null};
        int timeout = 1000;

        Thread t = new Thread(() ->
        {
            try
            {
                @SuppressWarnings("unchecked") TransferQueue<FakeResponse> fakeQueue =
                        (TransferQueue<FakeResponse>) mock(TransferQueue.class);
                when(fakeQueue.poll(timeout,
                                    TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException(
                        "foo"));

                @SuppressWarnings("unchecked") PBStreamingFutureOperation<FakeResponse, Void, Void>
                        coreFuture = (PBStreamingFutureOperation<FakeResponse, Void, Void>) mock(
                        PBStreamingFutureOperation.class);

                when(coreFuture.getResultsQueue()).thenReturn(fakeQueue);

                // ChunkedResponseIterator polls the response queue when created,
                // so we'll use that to simulate a Thread interrupt.
                new ChunkedResponseIterator<>(coreFuture,
                                              timeout,
                                              Long::new,
                                              FakeResponse::iterator);
            }
            catch (RuntimeException ex)
            {
                caught[0] = true;
                ie[0] = (InterruptedException) ex.getCause();
            }
            catch (InterruptedException e)
            {
                // Mocking TransferQueue::poll(timeout) requires this CheckedException be dealt with
                // If we actually catch one here we've failed at our jobs.
                caught[0] = false;
            }

            assertTrue(Thread.currentThread().isInterrupted());
        });

        t.start();
        t.join();

        assertTrue(caught[0]);
        assertEquals("foo", ie[0].getMessage());
    }

    static abstract class FakeResponse implements Iterable<Integer> {}
}
