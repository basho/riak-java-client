/*
 * Copyright 2013 Brian Roach <roach at basho dot com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


/**
 * @author Brian Roach <roach at basho dot com>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FutureOperation.class)
public class FutureOperationTest
{
    @Test
    public void setsRetrier() throws Exception
    {
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);

        operation.setRetrier(retrier, 1);
        OperationRetrier r = Whitebox.getInternalState(operation, "retrier");
        assertEquals(r, retrier);
    }

    @Test
    public void notifiesRetrier()
    {
        final int NUM_TRIES = 2;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);
        RiakResponse response = mock(RiakResponse.class);

        operation.setRetrier(retrier, NUM_TRIES);
        operation.setException(new Exception());
        verify(retrier).operationFailed(operation, NUM_TRIES - 1);

        operation.setResponse(response);
        verify(retrier).operationComplete(operation, NUM_TRIES - 2);
    }

    @Test
    public void exceptionWithNoRemainingRetriesIsDone()
    {
        final int NUM_TRIES = 1;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);

        operation.setRetrier(retrier, NUM_TRIES);
        operation.setException(new Exception());
        verify(retrier).operationFailed(operation, 0);
        assertTrue(operation.isDone());

    }

    @Test
    public void exceptionWithRemainingRetriesIsNotDone()
    {
        final int NUM_TRIES = 2;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);
        operation.setRetrier(retrier, NUM_TRIES);
        operation.setException(new Exception());
        assertFalse(operation.isDone());
    }

    @Test
    public void resultWithRemainingRetriesIsDone()
    {
        final int NUM_TRIES = 3;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);
        RiakResponse response = mock(RiakResponse.class);

        operation.setRetrier(retrier, NUM_TRIES);
        operation.setResponse(response);
        verify(retrier).operationComplete(operation, NUM_TRIES - 1);
        assertTrue(operation.isDone());
    }

    @Test
    public void isDoneAllowsGet() throws InterruptedException, ExecutionException
    {
        final int NUM_TRIES = 3;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);
        RiakResponse response = mock(RiakResponse.class);

        operation.setRetrier(retrier, NUM_TRIES);
        operation.setResponse(response);
        verify(retrier).operationComplete(operation, NUM_TRIES - 1);
        assertTrue(operation.isDone());
        assertNotNull(operation.get());
    }

    @Test(expected = TimeoutException.class)
    public void notDoneBlocksGet() throws InterruptedException, ExecutionException, TimeoutException
    {
        final int NUM_TRIES = 3;

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        OperationRetrier retrier = mock(OperationRetrier.class);

        operation.setRetrier(retrier, NUM_TRIES);
        operation.setException(new Exception());
        assertFalse(operation.isDone());
        operation.get(10, TimeUnit.MILLISECONDS);

    }

    @Test
    public void notifiesListenersAfterSuccess()
    {

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakResponse response = mock(RiakResponse.class);

        final AtomicBoolean called = new AtomicBoolean(false);
        operation.addListener(new RiakFutureListener()
        {
            @Override
            public void handle(RiakFuture f)
            {
                called.set(true);
            }
        });

        operation.setResponse(response);

        assertTrue(operation.isDone());
        assertTrue(called.get());

    }

    @Test
    public void notifiesLisetnersAfterFailure()
    {

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());

        final AtomicBoolean called = new AtomicBoolean(false);
        operation.addListener(new RiakFutureListener()
        {
            @Override
            public void handle(RiakFuture f)
            {
                called.set(true);
            }
        });

        operation.setException(new Exception());

        assertTrue(operation.isDone());
        assertTrue(called.get());

    }

    @Test
    public void notifiesOnAddAfterComplete()
    {

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakResponse response = mock(RiakResponse.class);

        operation.setResponse(response);

        final AtomicBoolean called = new AtomicBoolean(false);
        operation.addListener(new RiakFutureListener()
        {
            @Override
            public void handle(RiakFuture f)
            {
                called.set(true);
            }
        });

        assertTrue(operation.isDone());
        assertTrue(called.get());

    }

    @Test
    public void removedListenersDoNotGetCalled()
    {

        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakResponse response = mock(RiakResponse.class);

        final AtomicBoolean called = new AtomicBoolean(false);
        RiakFutureListener listener = new RiakFutureListener()
        {
            @Override
            public void handle(RiakFuture f)
            {
                called.set(true);
            }
        };

        operation.addListener(listener);
        operation.removeListener(listener);
        operation.setResponse(response);

        assertTrue(operation.isDone());
        assertFalse(called.get());

    }

    @Test(expected = IllegalStateException.class)
    public void canOnlySetSuccessOnce()
    {
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakResponse response = mock(RiakResponse.class);

        operation.setResponse(response);
        operation.setResponse(response);

    }

    @Test(expected = IllegalStateException.class)
    public void canOnlySetFailureOnce()
    {
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());

        operation.setException(new Exception());
        operation.setException(new Exception());

    }

    @Test(expected = IllegalStateException.class)
    public void canOnlySetSuccessOrFailure()
    {
        FutureOperation operation = PowerMockito.spy(new FutureOperationImpl());
        RiakResponse response = mock(RiakResponse.class);

        operation.setResponse(response);
        operation.setException(new Exception());

    }

    private class FutureOperationImpl extends FutureOperation<String>
    {
        public FutureOperationImpl()
        {
            supportedProtocols(Protocol.HTTP);
        }

        @Override
        protected String convert(RiakResponse rawResponse)
        {
            return "Fake!";
        }

        @Override
        protected Object createChannelMessage(Protocol p)
        {
            return "Fake!";
        }
    }


}
