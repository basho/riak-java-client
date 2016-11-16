package com.basho.riak.client.core;

import com.basho.riak.client.api.commands.ChunkedResponseIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.mockito.Matchers.any;

/**
 * @author Alex Moore <amoore at basho dot com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PBStreamingFutureOperation.class)
public class ChunkedResponseIteratorTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private TransferQueue<FakeResponse> fakeQueue;

    @Mock
    private PBStreamingFutureOperation<FakeResponse, Void, Void> coreFuture;

    private final int timeout = 1000;

    @Before
    public void initializeMOcks()
    {
        when(coreFuture.getResultsQueue()).thenReturn(fakeQueue);
    }

    /*
      NB: Disable this test if you want to use code coverage tools.
      The Thread interrupt it generates triggers a shutdown hook race bug in Java,
      which then doesn't allow EMMA to cleanly shutdown.
      https://bugs.openjdk.java.net/browse/JDK-8154017
     */
    @Test
    public void testInterruptedExceptionUponInitialisation() throws InterruptedException
    {
        Thread testThread = new Thread(() ->
        {
            try
            {
                when(fakeQueue.poll(any(Long.class), any(TimeUnit.class)))
                        .thenThrow(new InterruptedException())
                        .thenReturn(
                            new FakeResponse() {
                                @Override
                                public Iterator<Integer> iterator()
                                {
                                    return Arrays.asList(1,2,3,4,5).iterator();
                                }
                            });

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
        Thread testThread = new Thread(() ->
        {
            try
            {
                when(fakeQueue.poll(any(Long.class), any(TimeUnit.class)))
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
        Thread testThread = new Thread(() ->
        {
            try
            {
                when(coreFuture.isDone()).thenReturn(false);

                when(fakeQueue.poll(any(Long.class), any(TimeUnit.class)))
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

    @Test(timeout = 5000)
    public void checkProperIterationThroughChunkedResponse() throws InterruptedException {
        when(fakeQueue.poll(any(Long.class), any(TimeUnit.class)))
                // Simulate first chunk
                .thenReturn(
                        new FakeResponse() {
                            @Override
                            public Iterator<Integer> iterator()
                            {
                                return Arrays.asList(1,2).iterator();
                            }
                        })
                // Simulate next chunk
                .thenReturn(
                        new FakeResponse() {
                            @Override
                            public Iterator<Integer> iterator()
                            {
                                return Arrays.asList(3,4).iterator();
                            }
                        })
                // Simulate completion
                .thenAnswer(invocationOnMock ->
                {
                    when(coreFuture.isDone()).thenReturn(true);
                    when(fakeQueue.isEmpty()).thenReturn(true);
                    return null;
                });

        final ChunkedResponseIterator<Long, FakeResponse, Integer> iterator =
                new ChunkedResponseIterator<>(coreFuture, 50, Long::new, FakeResponse::iterator);

        assertEquals(1l, iterator.next().longValue());
        assertEquals(2l, iterator.next().longValue());
        assertEquals(3l, iterator.next().longValue());
        assertEquals(4l, iterator.next().longValue());

        assertFalse(iterator.hasNext());

        exception.expect(NoSuchElementException.class);
        iterator.next();
    }

    static abstract class FakeResponse implements Iterable<Integer> {}
}
