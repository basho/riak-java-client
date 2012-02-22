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
package com.basho.riak.client.raw.pbc;

import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakClientFactory;
import com.basho.riak.client.raw.config.Configuration;

import java.io.IOException;

public class PBFailoverClusterClientFactory implements RiakClientFactory
{

  private static final PBFailoverClusterClientFactory instance = new PBFailoverClusterClientFactory();

  private PBFailoverClusterClientFactory() {}

  public static PBFailoverClusterClientFactory getInstance() {
    return instance;
  }

  public boolean accepts(Class<? extends Configuration> configClass) {
    return configClass != null && configClass.equals(PBClusterConfig.class);
  }

  public RawClient newClient(Configuration config) throws IOException
  {
    PBFailoverClusterConfig conf = (PBFailoverClusterConfig) config;
    return new PBFailoverClusterClient(conf);
  }

}
