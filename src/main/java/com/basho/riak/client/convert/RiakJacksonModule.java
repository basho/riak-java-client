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
package com.basho.riak.client.convert;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;

/**
 * A Jackson module that customizes Jackson's object
 * mapper so we can handle Riak annotations like {@literal @RiakKey},
 * {@literal @RiakUsermeta}, etc correctly.
 *
 * Adds a {@link RiakBeanSerializerModifier} that removes any {@literal @RiakXXX} annotated
 * fields from the JSON output (since they will be persisted as object meta data
 * and not as part of the object). Explicitly adding an additional {@literal @JsonProperty}
 * annotation overrides this exclusion. 
 *
 * @author russell
 *
 */
public class RiakJacksonModule extends Module
{

    private static final String NAME = "RiakJacksonModule";
    private static final Version VERSION = Version.unknownVersion();

    public RiakJacksonModule()
    {
    }

    @Override
    public String getModuleName()
    {
        return NAME;
    }

    @Override
    public Version version()
    {
        return VERSION;
    }

    @Override
    public void setupModule(SetupContext context)
    {
        context.addBeanSerializerModifier(RiakBeanSerializerModifier.getInstance());
    }

}
