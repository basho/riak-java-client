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
 * A filter that matches keys whose name ends with the configured String argument
 * @author russell
 *
 */
public class EndsWithFilter implements KeyFilter {

    private static final String NAME = "ends_with";
    private final String[] filter;
    
    /**
     * Filter in keys that end with <code>endsWith</code>
     * @param endsWith
     */
    public EndsWithFilter(String endsWith) {
        filter = new String[] {NAME, endsWith};
    }
    
    /* (non-Javadoc)
     * @see com.basho.riak.client.query.filter.KeyFilter#asArray()
     */
    public String[] asArray() {
        return filter.clone();
    }
}
