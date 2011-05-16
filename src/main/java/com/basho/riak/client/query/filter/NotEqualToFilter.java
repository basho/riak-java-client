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
package com.basho.riak.client.query.filter;


/**
 * Filetr in keys that don't equal the configured argument
 * @author russell
 *
 */
public class NotEqualToFilter implements KeyFilter {
    private final static String NAME = "neq";
    private final Object[] filter;

    /**
     * @param equalTo
     */
    public NotEqualToFilter(String equalTo) {
        filter = new String[] { NAME, equalTo };
    }

    /**
     * @param equalTo
     */
    public NotEqualToFilter(int equalTo) {
        filter = new Object[] { NAME, equalTo };
    }

    /**
     * @param equalTo
     */
    public NotEqualToFilter(double equalTo) {
        filter = new Object[] { NAME, equalTo };
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.query.filter.KeyFilter#asArray()
     */
    public Object[] asArray() {
        return filter.clone();
    }
}
