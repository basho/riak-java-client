/*
 * Copyright 2013 Basho Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.query.search;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class YokozunaSchema
{
    private final String name;
    private final String content;
    
    /**
     * Constructs a YokozunaSchema.
     * Both name and content must be UTF-8 encoded strings.
     * @param name the name of the schema
     * @param content the XML that defines the schema
     */
    public YokozunaSchema(String name, String content)
    {
        if ((null == name || name.length() == 0) ||
            (null == content || content.length() == 0))
        {
            throw new IllegalArgumentException("Name and schema cannot be null or zero length");
        }
        
        this.name = name;
        this.content = content;
    }
    
    /**
     * Returns the name of this schema.
     * @return a UTF-8 string containing the name of this schema
     */
    public String getName()
    {
        return name;
    }
    
    /**
     * Returns the XML that defines this schema.
     * @return A UTF-8 encoded string containing the XML that defines this schema.
     */
    public String getContent()
    {
        return content;
    }
    
}
