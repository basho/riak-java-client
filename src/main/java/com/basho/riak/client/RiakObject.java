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
package com.basho.riak.client;

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;


public interface RiakObject {

    public void copyData(RiakObject object);

    public void updateMeta(String vclock, String lastmod, String vtag);

    public String getBucket();

    public String getKey();

    public String getValue();

    public Collection<RiakLink> getLinks();

    public Map<String, String> getUsermeta();

    public String getContentType();

    public String getVclock();

    public String getLastmod();

    public String getVtag();

    public String getEntity();

    public InputStream getEntityStream();

    public long getEntityStreamLength();
}