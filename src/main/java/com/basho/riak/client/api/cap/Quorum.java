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
package com.basho.riak.client.api.cap;

/**
 * Encapsulates a r/w/dw/rw/pr/pw quorum.
 * 
 * <p><h4>Symbolic Consistency Names</h4>
 * Riak 0.12 introduced “symbolic” consistency options for R and W 
 * that can be easier to use and understand. They are:
 * </p>
 * <ul>
 * <li>
 * all - All replicas must reply. This is the same as setting R or W equal to N.
 * </li>
 * <li>
 * one - This is the same as sending 1 as the R or W value.
 * </li>
 * <li>
 * quorum - A majority of the replicas must respond, that is, “half plus one”. 
 *          For the default N value of 3, this calculates to 2.
 * </li>
 * <li>
 * default - Uses whatever the per-bucket consistency property is for R or W, 
 *          which may be any of the above values, or an integer.
 * </li>
 * </ul>
 * <p>
 * Static factory methods are provided as a convenience for using these. 
 * </p>
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 * @see <a href="http://docs.basho.com/riak/latest/dev/advanced/cap-controls/">CAP Controls</a>
 */
public final class Quorum
{
    
    public static final String ONE = "one";
    public static final String QUORUM = "quorum";
    public static final String ALL = "all";
    public static final String DEFAULT = "default";
    
    private final int i;
    
    /**
     * Construct an instance using an integer value.
     * 
     * @param i the quorum value
     */
    public Quorum(int i)
    {
        if (i < -5 || i == -1)
        {
            throw new IllegalArgumentException("Illegal value for quorum: " + i);
        }
        this.i = i;
    }

    /**
     * Static factory method for a quorum of "one"
     * @return a new Quorum with an integer value of -2
     */
    public static Quorum oneQuorum()
    {
        return new Quorum(-2);
    }
    
    /**
     * Static factory method for a quorum of "quorum"
     * @return a new Quorum with an integer value of -3
     */
    public static Quorum quorumQuorum()
    {
        return new Quorum(-3);
    }
    
    /**
     * Static factory method for a quorum of "all"
     * @return a new Quorum with an integer value of -4
     */
    public static Quorum allQuorum()
    {
        return new Quorum(-4);
    }
    
    /**
     * Static factory method for a quorum of "default"
     * @return a new Quorum with an integer value of -5
     */
    public static Quorum defaultQuorum()
    {
        return new Quorum(-5);
    }
    
    /**
     * Determine if the quorum has a symbolic value.
     * @return true if this Quorum represents a symbolic value, false if literal
     *         integer value
     */
    public boolean isSymbolic()
    {
        switch(i)
        {
            case -2: // "one"
            case -3: // "quorum"
            case -4: // "all"
            case -5: // "default"
                return true;
            default:
                return false;
        }
    }

    /**
     * The int value of the quorum. Call isSymbolic to determine if you should
     * use this.
     * 
     * @return the int value. Will be a negative number for symbolic values.
     */
    public int getIntValue()
    {
        return i;
    }

    @Override
    public String toString()
    {
        String quorum = null;
        if (i > 0)
        {
            quorum = Integer.toString(i);
        }
        else
        {
            
            switch(i)
            {
                case -2:
                    quorum = ONE;
                    break;
                case -3:
                    quorum = QUORUM;
                    break;
                case -4:
                    quorum = ALL;
                    break;
                case -5:
                    quorum = DEFAULT;
                    break;
                default:
                    break;
            }
        }
        
        return quorum;
    }

    @Override public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + i;
        return result;
    }

    @Override public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof Quorum))
        {
            return false;
        }
        Quorum other = (Quorum) obj;
        if (i != other.i)
        {
            return false;
        }
        
        return true;
    }
}
