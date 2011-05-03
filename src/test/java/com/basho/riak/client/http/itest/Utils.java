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
package com.basho.riak.client.http.itest;

import static org.junit.Assert.*;

import org.apache.commons.httpclient.URIException;

import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.HttpResponse;

public class Utils {

    public static RequestMeta WRITE_3_REPLICAS() { return RequestMeta.writeParams(3, 3); }

    public static void assertSuccess(HttpResponse response) {
        if (!response.isSuccess()) {
            StringBuilder msg = new StringBuilder("Failed ");
            msg.append(response.getHttpMethod().getName()).append(" ");
            try {
                msg.append(response.getHttpMethod().getURI().toString());
            } catch (URIException e) {
                msg.append(response.getHttpMethod().getPath());
            }
            msg.append(" -- ")
                .append(response.getHttpMethod().getStatusLine()).append("; ")
                .append("Response headers: ").append(response.getHttpHeaders().toString()).append("; ")
                .append("Response body: ").append(new String(response.getBody()));
            fail(msg.toString());
        }
    }

}
