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
package com.basho.riak.client.query.functions;

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

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((function == null) ? 0 : function.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NamedJSFunction)) {
            return false;
        }
        NamedJSFunction other = (NamedJSFunction) obj;
        if (function == null) {
            if (other.function != null) {
                return false;
            }
        } else if (!function.equals(other.function)) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override public String toString() {
        return String.format("NamedJSFunction [function=%s]", function);
    }
}
