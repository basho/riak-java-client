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
 * @author russell
 * 
 */
public class StoreMeta {
    private final Integer w;
    private final Integer dw;
    private final Boolean returnBody;

    public StoreMeta(Integer w, Integer dw, Boolean returnBody) {
        this.w = w;
        this.dw = dw;
        this.returnBody = returnBody;
    }

    /**
     * @return
     * @see com.basho.riak.client.newapi.cap.StoreCAP#getW()
     */
    public Integer getW() {
        return w;
    }

    public boolean hasW() {
        return w != null;
    }

    /**
     * @return
     * @see com.basho.riak.client.newapi.cap.StoreCAP#getDW()
     */
    public Integer getDw() {
        return dw;
    }

    public boolean hasDW() {
        return dw != null;
    }

    public boolean hasReturnBody() {
        return returnBody != null;
    }

    /**
     * @return the returnBody
     */
    public Boolean getReturnBody() {
        return returnBody;
    }

}
