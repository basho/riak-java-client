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
 * Filter in keys that are less than the configured value
 * @author russell
 *
 */
public class LessThanFilter implements KeyFilter {
    
    private static final String NAME = "less_than";
    
    private final Object[] filter;

    /**
     * @param lessThan
     */
    public LessThanFilter(String lessThan) {
        filter = new String[] {NAME, lessThan};
    }

    /**
     * @param lessThan
     */
    public LessThanFilter(int lessThan) {
        filter = new Object[] {NAME, lessThan};
    }

    /**
     * @param lessThan
     */
    public LessThanFilter(double lessThan) {
        filter = new Object[] {NAME, lessThan};
    }

    /* (non-Javadoc)
     * @see com.basho.riak.client.query.filter.KeyFilter#asArray()
     */
    public Object[] asArray() {
        return filter.clone();
    }
}
