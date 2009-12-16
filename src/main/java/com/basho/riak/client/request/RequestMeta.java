/*
This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.  
*/
package com.basho.riak.client.request;

import java.util.Collections;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.basho.riak.client.util.Constants;

public class RequestMeta {
    
    private Map<String, String> queryParams = new LinkedHashMap<String, String>(); 

    public static RequestMeta readParams(int r) {
        RequestMeta meta = new RequestMeta();
        meta.addQueryParam(Constants.QP_R, Integer.toString(r));
        return meta;
    }

    public static RequestMeta writeParams(Integer w, Integer dw) {
        RequestMeta meta = new RequestMeta();
        if (w != null) meta.addQueryParam(Constants.QP_W, Integer.toString(w));
        if (dw != null) meta.addQueryParam(Constants.QP_DW, Integer.toString(dw));
        return meta;
    }

    public void put(String key, String value) { headers.put(key, value); }
    public String get(String key) { return headers.get(key); }
    private Map<String, String> headers = new HashMap<String, String>();

    public boolean contains(String key) {
        return headers.containsKey(key);
    }

    public Map<String, String> getHttpHeaders() { 
        return Collections.unmodifiableMap(headers); 
    }
    
    public String getQueryParam(String param) {
        return this.queryParams.get(param);
    }

    public String getQueryParams() {
        StringBuilder qp = new StringBuilder();
        for (String param : this.queryParams.keySet()) {
            if (qp.length() > 0) qp.append("&");
            qp.append(param).append("=").append(this.queryParams.get(param));
        }
        return qp.toString();
    }
    
    public void addQueryParam(String param, String value) {
        this.queryParams.put(param, value);
    }
}
