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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 * |   isSuccess() = false    |----+---->      isDone() = <b>true</b>      |
 * | isCancelled() = false    |    |    |   isSuccess() = <b>false</b>     |
 * |       cause() = null     |    |    |       cause() = <b>non-null</b>  |
 * +--------------------------+    |    +===========================+
 *                                 |    | Completed by cancellation |
 *                                 |    +---------------------------+
 *                                 +---->      isDone() = <b>true</b>      |
 *                                      | isCancelled() = <b>true</b>      |
 *                                      +---------------------------+
 * </pre>
 * </p>
 * <p>
 * The typical use pattern is to call {@literal await()}, check {@literal isSuccess()}
 * then call {@literal getNow()} or {@literal cause()}</p>
 * @author Brian Roach <roach at basho dot com>
 * @param <V> the (response) return type
 * @param <T> The query info type 
 * @since 2.0
 */
public interface RiakFuture<V, T> extends Future<V>
{
    /**
     * Not supported due to limitations of the Riak API.
     * <p>
     * At present time there is no way to cancel an operation sent to Riak.
     * This method will never succeed and always return false. 
     * </p>
     * @param mayInterruptIfRunning
     * @return always false.
     */
    @Override
    boolean cancel(boolean mayInterruptIfRunning);
    
    /**
     * Waits for this RiakFuture to be completed if necessary and returns the response if available.
     * @return The response from the operation.
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @throws ExecutionException if the computation threw an
     * exception
     */
    @Override
    V get() throws InterruptedException, ExecutionException;
    
    /**
     * Waits if necessary for at most the given time for the computation
     * to complete, and then retrieves its result, if available.
     * <p>
     * Note that the timeout value here is how long <b>you</b> are willing to wait
     * for this RiakFuture to complete. If you wish to set a timeout on the command 
     * itself, use the timeout() method provided in the command's associated builder.
     * </p>
     * @param timeout the amount of time to wait before returning.
     * @param unit the unit of time.
     * @return the response, or null if not completed or failed.
     * @throws InterruptedException if the current thread was interrupted
     * while waiting.
     * @throws ExecutionException if the computation threw an
     * exception.
     * @throws TimeoutException if the wait timed out.
     */
    @Override
    V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;
    @Override
    boolean isCancelled();
    @Override
    boolean isDone();
    /**
     * Waits for this RiakFuture to be completed.
     * <p>
     * Upon returning, the operation has completed. Checking isSuccess() tells 
     * you if it did so successfully.
     * </p>
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @see #isSuccess() 
     */
    void await() throws InterruptedException;
    /**
     * Waits for this RiakFuture to be completed for a set amount of time.
     * <p>Note that the timeout value here is how long <b>you</b> are willing to wait
     * for this RiakFuture to complete. Upon return you can check {@literal isDone()}
     * to see if the future has completed yet or not. The operation is still in
     * progress if that returns false.
     * </p>
     * <p>
     * If you wish to set a timeout on the command itself, use the timeout() method
     * provided in the command's associated builder.
     * </p>
     * 
     * @param timeout the amount of time to wait before returning.
     * @param unit the unit of time.
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @see #isDone() 
     * @see #isSuccess() 
     */
    void await(long timeout, TimeUnit unit) throws InterruptedException;
    /**
     * Determine if the operation was successful.
     * <p>
     * In the case of true, get() will then return the response. If false, 
     * cause() will then return non-null.
     * @return true if completed and successful, false otherwise.
     * @see #cause() 
     */
    
    /**
     * Return the result without blocking or throwing an exception. 
     * If the future is not yet done or has failed this will return null. 
     * As it is possible that a null value is used to mark the future as successful 
     * you also need to check if the future is really done with {@link #isDone()}  
     * and not rely on the returned null value.
     * @return The response, or {@literal null} if the future is not yet completed or failed.
     * @see #isDone() 
     * @see #isSuccess() 
     */
    V getNow();
    /**
     * Determine if the operation completed successfully.
     * @return true if the operation succeeded, false if not yet completed or failed.
     */
    boolean isSuccess();
    /**
     * Return information about the operation and why it failed.
     * <p>
     * Note this will return {@literal null} if the operation completed 
     * successfully.
     * </p>
     * @return The exception thrown during the operation, or null if not set.
     * @see #isSuccess() 
     */
    Throwable cause();
    /**
     * Returns information related to the operation performed.
     * <p>
     * Useful in async operations when you want to know what operation this
     * future refers to.
     * <p>
     * @return Information pertaining to the operation attached to this future.
     */
    T getQueryInfo();
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
