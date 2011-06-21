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
package com.basho.riak.client.http;

import java.util.regex.Pattern;

/**
 * unite host names for testing. <br/>
 * your environment is not {@code 127.0.0.1}, you also modify app.config.<br/>
 * there is a example. in this case, your riak server is {@code 192.168.0.2}.
 * 
 * <pre>
 * from {http, [ {"127.0.0.1", 8098 } ]},
 * to   {http, [ {"127.0.0.1", 8098 }, {"192.168.0.2",8098} ]},
 * 
 * from {pb_ip,    "127.0.0.1" },
 * to   {pb_ip,    "192.168.0.2" },
 * </pre>
 * and then use there System properties.<br/>
 * <ul>
 * <li>{@code com.basho.riak.host}<ul>riak server host name</ul>
 * <li>{@code com.basho.riak.pbc.port}<ul>riak server protocol buffers port</ul>
 * <li>{@code com.basho.riak.http.port}<ul>riak server REST port</ul>
 * </ul>
 * set System properties like this.
 * <pre>
 * -Dcom.basho.riak.host=192.168.0.2
 * </pre>
 * @author taichi
 */
public class Hosts {

	public static final String PREFIX = "com.basho.riak.";
	public static final String KEY_HOST = PREFIX + "host";
	public static final String KEY_PBC_PORT = PREFIX + "pbc.port";
	public static final String KEY_HTTP_PORT = PREFIX + "http.port";

	public static final String RIAK_HOST;

	public static final int RIAK_PORT;

	static {
		RIAK_HOST = System.getProperty(KEY_HOST, "127.0.0.1");
		String port = System.getProperty(KEY_PBC_PORT);
		if (port != null && Pattern.matches("\\d+", port)) {
			RIAK_PORT = Integer.parseInt(port);
		} else {
			RIAK_PORT = 8087;
		}
	}
	
	public static final String RIAK_URL;

	static {
		StringBuilder stb = new StringBuilder();
		stb.append("http://");
		stb.append(RIAK_HOST);
		stb.append(":");
		stb.append(System.getProperty(KEY_HTTP_PORT, "8098"));
		stb.append("/riak");
		RIAK_URL = stb.toString();
	}

}
