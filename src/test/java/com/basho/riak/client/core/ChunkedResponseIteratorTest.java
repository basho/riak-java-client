package com.basho.riak.client.core;


import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * @author Alex Moore <amoore at basho dot com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PBStreamingFutureOperation.class)
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
        final Throwable[] ex = {null};
        int timeout = 1000;

        Thread testThread = new Thread(() ->
        {
            try
            {
                @SuppressWarnings("unchecked")
                TransferQueue<FakeResponse> fakeQueue =
                        (TransferQueue<FakeResponse>) mock(TransferQueue.class);

                when(fakeQueue.poll(timeout, TimeUnit.MILLISECONDS)).thenThrow(new InterruptedException("foo"));

                @SuppressWarnings("unchecked")
                PBStreamingFutureOperation<FakeResponse, Void, Void> coreFuture =
                        (PBStreamingFutureOperation<FakeResponse, Void, Void>) mock(PBStreamingFutureOperation.class);

                when(coreFuture.getResultsQueue()).thenReturn(fakeQueue);

                // ChunkedResponseIterator polls the response queue when created,
                // so we'll use that to simulate a Thread interrupt.
                new ChunkedResponseIterator<>(coreFuture,
                                              timeout,
                                              Long::new,
                                              FakeResponse::iterator);
            }
            catch (RuntimeException rex)
            {
                ex[0] = rex;
            }
            catch (InterruptedException e)
            {
                // Mocking TransferQueue::poll(timeout) requires this CheckedException be dealt with
                // If we actually catch one here we've failed at our jobs.
                fail(e.getMessage());
            }

            assertTrue(Thread.currentThread().isInterrupted());
        });

        testThread.start();
        testThread.join();

        Throwable caughtException = ex[0];
        assertNotNull(caughtException);

        Throwable wrappedException = caughtException.getCause();
        assertNotNull(caughtException.getMessage(), wrappedException);
        assertEquals("foo", wrappedException.getMessage());
        assertTrue(wrappedException instanceof InterruptedException);
    }

    static abstract class FakeResponse implements Iterable<Integer> {}
}
