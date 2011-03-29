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
package com.basho.riak.newapi.query;

/**
 * Models a named erlang function.
 * 
 * Immutable.
 * 
 * @author russell
 * 
 */
public class NamedErlangFunction implements NamedFunction {
    private final String mod;
    private final String fun;

    /**
     * @param mod
     *            the module that contains the function.
     * @param fun
     *            the function name.
     */
    public NamedErlangFunction(String mod, String fun) {
        this.mod = mod;
        this.fun = fun;
    }

    /**
     * @return the erlang module that contains the function.
     */
    public String getMod() {
        return mod;
    }

    /**
     * @return the function name.
     */
    public String getFun() {
        return fun;
    }

}
