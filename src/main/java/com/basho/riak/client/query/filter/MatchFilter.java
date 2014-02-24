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
 * Filter in keys that match the regular expression argument
 * @author russell
 *
 */
public class MatchFilter implements KeyFilter {

    private static final String NAME = "matches";
    private final String[] filter;

    /**
     * @param matchFilter a Reg Exp
     */
    public MatchFilter(String matchFilter) {
        filter = new String[] { NAME, matchFilter };
    }

    public String[] asArray() {
        return filter.clone();
    }
}
