/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.cap;

import java.util.concurrent.Callable;

import com.basho.riak.client.RiakRetryFailedException;

/**
 * A basic retrier implementation. Construct it with the number of times a
 * {@link Callable} should be attempted. When <code>attempt</code> is called
 * with a {@link Callable} then {@link Callable#call()} is run
 * <code>attempts</code> times before throwing a
 * {@link RiakRetryFailedException}. It is important to note that there is no
 * backoff between attempts.
 * 
 * @author russell
 */
public class DefaultRetrier implements Retrier {

    private final int attempts;

    /**
     * @param attempts
     *            how many times the retrier should attempt the call before
     *            throwing a {@link RiakRetryFailedException}
     */
    public DefaultRetrier(int attempts) {
        this.attempts = attempts;
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.cap.Retrier#attempt(java.util.concurrent.Callable)
     */
    public <T> T attempt(Callable<T> command) throws RiakRetryFailedException {
        return attempt(command, attempts);
    }

    /**
     * Calls {@link Callable#call()} <code>times</code> before giving up and
     * throwing a {@link RiakRetryFailedException} There is no back off.
     *
     * @param <T>
     *            the {@link Callable}'s return type.
     * @param command
     *            the {@link Callable} to attempt
     * @param times
     *            how many times to try before we throw
     * @return the result of command
     * @throws RiakRetryFailedException
     *             if the Callable throws an exception <code>times</code> times
     */
    public static <T> T attempt(final Callable<T> command, int times) throws RiakRetryFailedException {
        try {
            return command.call();
        } catch (Exception e) {
            if (times == 0) {
                throw new RiakRetryFailedException(e);
            } else {
                return attempt(command, times--);
            }
        }
    }

    /**
     * Static factory method to create a default retrier
     *
     * @param attempts
     *            how many times the {@link DefaultRetrier} will attempt to call
     *            a {@link Callable} supplied to
     *            {@link Retrier#attempt(Callable)}
     * @return a {@link DefaultRetrier} configured for <code>attempts</code>
     *         retries
     */
    public static Retrier attempts(int attempts) {
        return new DefaultRetrier(attempts);
    }
}
