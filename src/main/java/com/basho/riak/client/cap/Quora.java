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
package com.basho.riak.client.cap;

/**
 * An enum that models the set of symbolic quora.
 * 
 * @author russell
 * 
 */
public enum Quora {
    /*
     * Riak uses certain "magic" numbers in the protocol buffers transport. 
     * They are unsigned 32 bit ints:
     * 'one' (4294967295-1), 'quorum' (4294967295-2), 'all' (4294967295-3), 'default' (4294967295-4)
     * The way java protocol buffers works is that it uses a (signed) java int
     * on the java side, casting it to unsigned on the other (erlang) side
     */
    
     /*
      * "one" = (int)(4294967295L - 1L); 
      * "quorum" = (int)(4294967295L - 2L); 
      * "all" = (int)(4294967295L - 3L); 
      * "default" =  (int)(4294967295L - 4L); 
      */ 
    
    
    ONE(-2, "one"), QUORUM(-3, "quorum"), ALL(-4, "all"), 
    DEFAULT(-5, "default"), INTEGER(Integer.MIN_VALUE, "int");

    private final int value;
    private final String name;

    private Quora(int value, String name) {
        this.value = value;
        this.name = name;
    }

    /**
     * @return the value
     */
    public int getValue() {
        return value;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Attempt to return a {@link Quora} for the given <code>name</code>
     * @param name
     * @return a {@link Quora} for <code>name</name>
     * @throws IllegalArgumentException is <code>name</code> is not a valid Quora.
     */
    public static Quora fromString(String name) {
        for(Quora q : values()) {
            if(q.getName().equals(name)) {
                return q;
            }
        }
        throw new IllegalArgumentException(name + " is not a known value for a Quorum");
    }
}
