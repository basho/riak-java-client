/*
 * Copyright 2013-2016 Basho Technologies Inc
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
package com.basho.riak.client.api.commands;

import com.basho.riak.client.core.FutureOperation;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

/**
 * Operation Tests Base for the operation that returns something and requires conversion.
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.6
 */
public abstract class MockedResponseOperationTest<O extends FutureOperation, R> extends OperationTestBase<O>
{
    private final Class<R> responseClass;
    private R mockResponse;

    public MockedResponseOperationTest(Class<R> responseClass)
    {
        super();
        this.responseClass = responseClass;
    }

    @Override
    public void setup() throws ExecutionException, InterruptedException, TimeoutException
    {
        super.setup();

        mockResponse = Mockito.mock(responseClass);
        when(mockFuture.get()).thenReturn(mockResponse);
        when(mockFuture.get(anyLong(), any(TimeUnit.class))).thenReturn(mockResponse);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(mockFuture.isDone()).thenReturn(true);

        setupResponse(mockedResponse());
    }

    protected final R mockedResponse()
    {
        return mockResponse;
    }

    protected void setupResponse(R mockedResponse)
    {
    }
}
