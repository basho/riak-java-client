package com.basho.riak.client.core;


import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.*;
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
    public void testInterruptedExceptionUponInitialisation() throws InterruptedException
    {
        int timeout = 1000;

        Thread testThread = new Thread(() ->
        {
            try
            {
                @SuppressWarnings("unchecked")
                TransferQueue<FakeResponse> fakeQueue =
                        (TransferQueue<FakeResponse>) mock(TransferQueue.class);

                when(fakeQueue.poll(timeout, TimeUnit.MILLISECONDS))
                        .thenThrow(new InterruptedException())
                        .thenReturn(
                            new FakeResponse() {
                                @Override
                                public Iterator<Integer> iterator()
                                {
                                    return Arrays.asList(1,2,3,4,5).iterator();
                                }
                            });

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
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void testInterruptedExceptionUponNextChunkLoad() throws InterruptedException
    {
        int timeout = 1000;

        Thread testThread = new Thread(() ->
        {
            try
            {
                @SuppressWarnings("unchecked")
                TransferQueue<FakeResponse> fakeQueue =
                        (TransferQueue<FakeResponse>) mock(TransferQueue.class);

                when(fakeQueue.poll(timeout, TimeUnit.MILLISECONDS))
                        .thenReturn(
                                new FakeResponse() {
                                    @Override
                                    public Iterator<Integer> iterator()
                                    {
                                        return Collections.singletonList(1).iterator();
                                    }
                                })
                        .thenThrow(new InterruptedException())
                        .thenReturn(
                                new FakeResponse() {
                                    @Override
                                    public Iterator<Integer> iterator()
                                    {
                                        return Collections.singletonList(2).iterator();
                                    }
                                });

                @SuppressWarnings("unchecked")
                PBStreamingFutureOperation<FakeResponse, Void, Void> coreFuture =
                        (PBStreamingFutureOperation<FakeResponse, Void, Void>) mock(PBStreamingFutureOperation.class);

                when(coreFuture.getResultsQueue()).thenReturn(fakeQueue);

                ChunkedResponseIterator<Long, FakeResponse, Integer> chunkedResponseIterator =
                        new ChunkedResponseIterator<>(coreFuture, timeout, Long::new, FakeResponse::iterator);

                assertTrue(chunkedResponseIterator.hasNext());
                assertEquals(new Long(1), chunkedResponseIterator.next());
                // Should hit InterruptedException here, and then take care of it.
                assertTrue(chunkedResponseIterator.hasNext());
                assertEquals(new Long(2), chunkedResponseIterator.next());
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
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    public void testConcurrentDoneAndInterruptedException() throws InterruptedException
    {
        int timeout = 1000;

        Thread testThread = new Thread(() ->
        {
            try
            {
                @SuppressWarnings("unchecked")
                PBStreamingFutureOperation<FakeResponse, Void, Void> coreFuture =
                        (PBStreamingFutureOperation<FakeResponse, Void, Void>) mock(PBStreamingFutureOperation.class);
                when(coreFuture.isDone()).thenReturn(false);

                @SuppressWarnings("unchecked")
                TransferQueue<FakeResponse> fakeQueue =
                        (TransferQueue<FakeResponse>) mock(TransferQueue.class);

                when(fakeQueue.poll(timeout, TimeUnit.MILLISECONDS))
                        .thenReturn(
                                new FakeResponse() {
                                    @Override
                                    public Iterator<Integer> iterator()
                                    {
                                        return Collections.singletonList(1).iterator();
                                    }
                                })
                        .thenAnswer(invocationOnMock ->
                                    {
                                        when(coreFuture.isDone()).thenReturn(true);
                                        when(fakeQueue.isEmpty()).thenReturn(true);
                                        throw new InterruptedException();
                                    });

                when(coreFuture.getResultsQueue()).thenReturn(fakeQueue);

                ChunkedResponseIterator<Long, FakeResponse, Integer> chunkedResponseIterator =
                        new ChunkedResponseIterator<>(coreFuture, timeout, Long::new, FakeResponse::iterator);

                assertTrue(chunkedResponseIterator.hasNext());
                assertEquals(new Long(1), chunkedResponseIterator.next());
                // InterruptedException should happen when we try to load the next chunk,
                // But the catch + next attempt to load a chunk should check to see if we're done.
                assertFalse(chunkedResponseIterator.hasNext());

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
        assertFalse(Thread.currentThread().isInterrupted());
    }

    static abstract class FakeResponse implements Iterable<Integer> {}
}
