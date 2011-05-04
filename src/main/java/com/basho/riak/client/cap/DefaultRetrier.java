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

import java.io.IOException;

import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.raw.Command;

/**
 * @author russell
 * 
 */
public class DefaultRetrier implements Retrier {

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.basho.riak.client.spi.Retrier#attempt(com.basho.riak.client.spi.Command
     * )
     */
    public <T> T attempt(Command<T> command, int times) throws RiakRetryFailedException {
        try {
            return command.execute();
        } catch (IOException e) {
            if (times == 0) {
                throw new RiakRetryFailedException(e);
            } else {
                return attempt(command, times--);
            }
        }
    }

}
