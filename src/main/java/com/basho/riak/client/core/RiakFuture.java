/*
 * Copyright 2013 Basho Technologies Inc
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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The result of an asynchronous Riak operation. 
 * <p>
 * All Riak operations are asynchronous. It means when you execute an operation on
 * the cluster it will return immediately with no guarantee that the requested 
 * operation has been completed at the end of the call. Instead, you will be returned 
 * a {@code RiakFuture} instance which gives you the information about the result 
 * or status of the operation.
 * </p>
 * <p>
 * A {@code RiakFuture} is either uncompleted or completed. When an operation begins, a new future 
 * object is created. The new future is uncompleted initially - it is neither 
 * succeeded, failed, nor canceled because the operation is not finished yet. 
 * If the operation is finished either successfully, with failure, or by cancellation, 
 * the future is marked as completed with more specific information, such as the 
 * cause of the failure. Please note that even failure and cancellation belong to the completed state.
 * <pre>
 *                                      +---------------------------+
 *                                      | Completed successfully    |
 *                                      +---------------------------+
 *                                 +---->      isDone() = <b>true</b>      |
 * +--------------------------+    |    |   isSuccess() = <b>true</b>      |
 * |        Uncompleted       |    |    +===========================+
 * +--------------------------+    |    | Completed with failure    |
 * |      isDone() = <b>false</b>    |    |    +---------------------------+
 * |   isSuccess() = false    |----+---->   isDone() = <b>true</b>         |
 * | isCancelled() = false    |    |    |    cause() = <b>non-null</b>     |
 * |       cause() = null     |    |    +===========================+
 * +--------------------------+    |    | Completed by cancellation |
 *                                 |    +---------------------------+
 *                                 +---->      isDone() = <b>true</b>      |
 *                                      | isCancelled() = <b>true</b>      |
 *                                      +---------------------------+
 * </pre>
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public interface RiakFuture<V, T> extends Future<V>
{
    @Override
    boolean cancel(boolean mayInterruptIfRunning);
    @Override
    V get() throws InterruptedException;
    @Override
    V get(long timeout, TimeUnit unit) throws InterruptedException;
    @Override
    boolean isCancelled();
    @Override
    boolean isDone();
    /**
     * Waits for this RiakFuture to be completed.
     * @throws InterruptedException 
     */
    void await() throws InterruptedException;
    /**
     * Waits for this RiakFuture to be completed, or times out.
     * @param timeout the amount of time to wait before returning.
     * @param unit the unit of time.
     * @throws InterruptedException 
     */
    void await(long timeout, TimeUnit unit) throws InterruptedException;
    boolean isSuccess();
    /**
     * Return information about the operations and why it failed.
     * @return A FailureInfo instance containing information about the operation and why it failed.
     */
    FailureInfo<T> cause();
    /**
     * Add a listener to this RiakFuture.
     * @param listener a FiakFutureListener that will be notified when this RiakFuture completes.
     */
    void addListener(RiakFutureListener<V,T> listener);
    /**
     * Remove a listener from this RiakFuture.
     * @param listener The listener to remove.
     */
    void removeListener(RiakFutureListener<V,T> listener);
}
