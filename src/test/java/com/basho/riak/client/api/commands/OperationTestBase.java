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

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakFuture;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Operation Tests Base
 *
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 2.0.6
 */
@RunWith(MockitoJUnitRunner.class)
public abstract class OperationTestBase<O extends FutureOperation>
{
    @Mock
    protected RiakFuture mockFuture;

    @Mock
    private RiakCluster mockCluster;

    @Spy
    @InjectMocks
    private RiakClient client;

    @Captor
    private ArgumentCaptor<O> captor;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws ExecutionException, InterruptedException, TimeoutException
    {
        when(mockCluster.execute(any(FutureOperation.class))).thenReturn(mockFuture);
    }

    @SuppressWarnings("unchecked")
    protected final <C extends RiakCommand> O executeAndVerify(C command) throws ExecutionException, InterruptedException
    {
        client.execute(command);
        verify(mockCluster).execute(captor.capture());
        return captor.getValue();
    }
}
