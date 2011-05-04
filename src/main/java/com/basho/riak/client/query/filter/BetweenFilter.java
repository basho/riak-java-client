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


public class BetweenFilter implements KeyFilter {
    private static final String NAME = "between";

    private final Object[] filter;

    public BetweenFilter(String from, String to) {
        filter = new String[] { NAME, from, to };
    }

    public BetweenFilter(int from, int to) {
        filter = new Object[] { NAME, from, to };
    }

    public BetweenFilter(long from, long to) {
        filter = new Object[] { NAME, from, to };
    }

    public BetweenFilter(double from, double to) {
        filter = new Object[] { NAME, from, to };
    }

    public Object[] asArray() {
        return filter.clone();
    }
}
