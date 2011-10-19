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
package com.basho.riak.client;

import com.basho.riak.client.http.Hosts;

/**
 * @author russell
 * 
 */
public final class RiakTestProperties {

    public static final String SEARCH_PROPERTY = "search";
    public static final boolean SEARCH_ENABLED = Boolean.parseBoolean(System.getProperty(Hosts.PREFIX + SEARCH_PROPERTY,
                                                                                         "false"));
    
    public static final String _2I_PROPERTY = "2i";
    public static final boolean _2I_ENABLED = Boolean.parseBoolean(System.getProperty(Hosts.PREFIX + _2I_PROPERTY,
                                                                                         "false"));

    private RiakTestProperties() {};

    public static boolean isSearchEnabled() {
        return SEARCH_ENABLED;
    }

    public static boolean is2iEnabled() {
        return _2I_ENABLED;
    }
}
