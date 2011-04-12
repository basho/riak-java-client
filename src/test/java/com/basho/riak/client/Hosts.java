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

/**
 * unite host names for testing. <br/>
 * your environment is not {@code 127.0.0.1}, you also modify app.config.<br/>
 * there is a example. in this case, your riak server is {@code 192.168.0.2}.
 * <pre>
 * from {http, [ {"127.0.0.1", 8098 } ]},
 * to   {http, [ {"127.0.0.1", 8098 }, {"192.168.0.2",8098} ]},
 * 
 * from {pb_ip,    "127.0.0.1" },
 * to   {pb_ip,    "192.168.0.2" },
 * </pre>
 * 
 * @author taichi
 */
public class Hosts {

	public static final String RIAK_HOST = "127.0.0.1";

	public static final String RIAK_URL = "http://" + RIAK_HOST + ":8098/riak";
}
