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
package com.basho.riak.client.raw;

/**
 * Encapsulates the optional parameters for a store operation on Riak
 * @author russell
 * @see RawClient#store(com.basho.riak.client.IRiakObject, StoreMeta)
 */
public class StoreMeta {
    private final Integer w;
    private final Integer dw;
    private final Boolean returnBody;

    /**
     * Create a StoreMeta, accepts <code>null</code>s for any parameter
     * @param w the write quorum for a store operation
     * @param dw the durable write quorum for a store operation
     * @param returnBody should the store operation return the new data item and its meta data
     */
    public StoreMeta(Integer w, Integer dw, Boolean returnBody) {
        this.w = w;
        this.dw = dw;
        this.returnBody = returnBody;
    }

    /**
     * The write quorum
     * @return an Integer or null if no write quorum set
     */
    public Integer getW() {
        return w;
    }

    /**
     * Is the write quorum set?
     * @return <code>true</code> if the write quorum is set, <code>false</code> otherwise
     */
    public boolean hasW() {
        return w != null;
    }

    /**
     * The durable write quorum
     * @return an Integer or <code>null</code> if the durable write quorum is not set
     */
    public Integer getDw() {
        return dw;
    }

    /**
     * Has the durable write quorum been set?
     * @return <code>true</code> if durable write quorum is set, <code>false</code> otherwise
     */
    public boolean hasDW() {
        return dw != null;
    }

    /**
     * Has the return body parameter been set?
     * @return <code>true</code> of return body parameter is set, <code>false</code> otherwise
     */
    public boolean hasReturnBody() {
        return returnBody != null;
    }

    /**
     * Get the value for the return body parameter
     * @return the returnBody parameter or <code>null</code> if it is not set
     */
    public Boolean getReturnBody() {
        return returnBody;
    }

}
