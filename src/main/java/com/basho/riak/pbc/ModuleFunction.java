/*
 * Copyright 2013 Basho Technologies Inc
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
package com.basho.riak.pbc;

/**
 *
 * @author Brian Roach <roach at basho dot com>
 */
public class ModuleFunction
{
    private String module;
    private String function;

    public ModuleFunction(String module, String function) 
    {
        this.module = module;
        this.function = function;
    }
    
    /**
     * @return the module
     */
    public String getModule()
    {
        return module;
    }

    /**
     * @return the function
     */
    public String getFunction()
    {
        return function;
    }

}
