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
package com.basho.riak.newapi.query.functions;

/**
 * A named function that is a JS built in function.
 * 
 * @author russell
 *
 */
public class NamedJSFunction implements NamedFunction {
    
    private final String function;

    /**
     * @param function
     */
    public NamedJSFunction(String function) {
        this.function = function;
    }

    /**
     * @return the function
     */
    public String getFunction() {
        return function;
    }
}
