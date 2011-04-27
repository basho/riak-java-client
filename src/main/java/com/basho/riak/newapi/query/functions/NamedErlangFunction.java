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

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fun == null) ? 0 : fun.hashCode());
        result = prime * result + ((mod == null) ? 0 : mod.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NamedErlangFunction)) {
            return false;
        }
        NamedErlangFunction other = (NamedErlangFunction) obj;
        if (fun == null) {
            if (other.fun != null) {
                return false;
            }
        } else if (!fun.equals(other.fun)) {
            return false;
        }
        if (mod == null) {
            if (other.mod != null) {
                return false;
            }
        } else if (!mod.equals(other.mod)) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("NamedErlangFunction [mod=%s, fun=%s]", mod, fun);
    }

}
