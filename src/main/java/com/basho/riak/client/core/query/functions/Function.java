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
package com.basho.riak.client.core.query.functions;

/**
 * A Function for use with bucket properties or asMap reduce.
 * 
 * Instances are created via the provided static factory methods or by using the Builder.
 * @author Brian Roach <roach at basho dot com>
 * @since 2.0
 */
public class Function
{
    private final boolean isJavascript;
    
    private final String name;
    private final String bucket;
    private final String key;
    private final String source;
    
    private final String module;
    private final String function;
    
    private Function(Builder builder)
    {
        switch(builder.type)
        {
            case NAMED_JS:
            case STORED_JS:
            case ANON_JS:
                isJavascript = true;
                break;
            default:
                isJavascript = false;
                break;
        }
        
        this.name = builder.name;
        this.bucket = builder.bucket;
        this.key = builder.key;
        this.source = builder.source;
        this.module = builder.module;
        this.function = builder.function;
    }
    
    /**
     * Returns whether this function is Javascript or not.
     * @return true if this is a JS function, false if Erlang.
     */
    public boolean isJavascript()
    {
        return isJavascript;
    }
    
    // JS stuff
    /**
     * Return whether this is a named JS function or not.
     * @return true if this is a named JS function, false otherwise.
     */
    public boolean isNamed()
    {
        return name != null;
    }
    
    /**
     * Return whether this is a JS function stored as an object in Riak.
     * @return true if this is a stored JS function, false otherwise.
     */
    public boolean isStored()
    {
        return bucket != null;
    }
    
    /**
     * Return whether this is an anonymous JS function or not.
     * @return true if this is an anonymous JS function, false otherwise.
     */
    public boolean isAnonymous()
    {
        return source != null;
    }
    
    /**
     * Returns the name of this named JS function.
     * @return The name for this function or null if this is not a named JS function.
     */
    public String getName()
    {
        return name;
    }
    
    /**
     * Returns the bucket for this stored JS function.
     * @return The bucket for this function or null if this is not a stored JS function.
     */
    public String getBucket()
    {
        return bucket;
    }
    
    /**
     * Returns the key for this stored JS function.
     * @return The key for function or null is this is not a stored JS function.
     */
    public String getKey()
    {
        return key;
    }
    
    /**
     * Returns the source for this anonymous JS function.
     * @return The source for this function, or null if it is not an anonymous function.
     */
    public String getSource()
    {
        return source;
    }
    
    // Erlang
    /**
     * Returns the module for this Erlang function.
     * @return the module for this function or null if this is not an Erlang function.
     */
    public String getModule()
    {
        return module;
    }
    
    /**
     * Returns the function for this Erlang function.
     * @return the function or null if this is not an erlang function.
     */
    public String getFunction()
    {
        return function;
    }
    
    /**
     * Static factory method for Named Javascript Functions.
     * @param name the name of the Javascript function.
     * @return a Function representing a named JS Function.
     */
    public static Function newNamedJsFunction(String name)
    {
        return new Builder().withName(name).build();
    }
    
    /**
     * Static factory method for Stored Javascript Functions.
     * @param bucket The bucket where the JS function is stored
     * @param key the key for the object containing the JS function
     * @return a Function representing a stored JS function.
     */
    public static Function newStoredJsFunction(String bucket, String key)
    {
        return new Builder().withBucket(bucket).withKey(key).build();
    }
    
    /**
     * Static factory method for Anonymous JS Functions.
     * @param source the javascript source
     * @return a Function representing an anonymous JS function.
     */
    public static Function newAnonymousJsFunction(String source)
    {
        return new Builder().withSource(source).build();
    }
    
    /**
     * Static factory method for Erlang Functions.
     * @param module the module that contains the Erlang function.
     * @param function the name of the erlang function.
     * @return a Function representing a Erlang function.
     */
    public static Function newErlangFunction(String module, String function)
    {
        return new Builder().withModule(module).withFunction(function).build();
    }
    
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (null == name ? 0 : name.hashCode());
        result = prime * result + (null == bucket ? 0 : bucket.hashCode());
        result = prime * result + (null == key ? 0 : key.hashCode());
        result = prime * result + (null == source ? 0 : source.hashCode());
        result = prime * result + (null == module ? 0 : module.hashCode());
        result = prime * result + (null == function ? 0 : function.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof Function))
        {
            return false;
        }
        Function other = (Function) obj;
        if ( (name == other.name || (name != null && name.equals(other.name))) &&
             (bucket == other.bucket || (bucket != null && bucket.equals(other.bucket))) &&
             (key == other.key || (key != null && key.equals(other.key))) &&
             (source == other.source || (source != null && source.equals(other.source))) && 
             (module == other.module || (module != null && module.equals(other.module))) && 
             (function == other.function || (function != null && function.equals(other.function)))
           )
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    /**
     * Builder class for creating Function instances.
     */
    public static class Builder
    {
        private enum Type
        {
            UNKNOWN,
            NAMED_JS,
            STORED_JS,
            ANON_JS,
            ERLANG
        }
        private Type type = Type.UNKNOWN;
    
        private String name;
        private String bucket;
        private String key;
        private String source;

        private String module;
        private String function;
        
        
        
        public Builder()
        {
            
        }
        
        /**
         * Set the name for a Named Javascript function.
         * @param name the name of the function.
         * @return a reference to this builder.
         */
        public Builder withName(String name)
        {
            stringCheck(name);
            switch(type)
            {
                case UNKNOWN:
                case NAMED_JS:
                    this.name = name;
                    type = Type.NAMED_JS;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a name.");
            }
            
            return this;
        }
        
        /**
         * Set the bucket for a stored JS function.
         * @param bucket the name of the bucket where this function is stored.
         * @return a reference to this builder.
         */
        public Builder withBucket(String bucket)
        {
            stringCheck(bucket);
            switch(type)
            {
                case UNKNOWN:
                case STORED_JS:
                    this.bucket = bucket;
                    type = Type.STORED_JS;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a bucket.");
            }
            
            return this;
        }
        
        /**
         * Set the key for a stored JS function.
         * @param key the key for the object that contains the JS function.
         * @return a reference to this builder.
         */
        public Builder withKey(String key)
        {
            stringCheck(key);
            switch(type)
            {
                case UNKNOWN:
                case STORED_JS:
                    this.key = key;
                    type = Type.STORED_JS;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a key.");
            }
            return this;
        }
        
        /**
         * Set the source for an anonymous JS function
         * @param source the Javascript source.
         * @return a reference to this builder.
         */
        public Builder withSource(String source)
        {
            stringCheck(source);
            switch(type)
            {
                case UNKNOWN:
                    this.source = source;
                    type = Type.ANON_JS;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a source.");
            }
            return this;
        }
        
        /**
         * Set the module for an Erlang function.
         * @param module the name of the module containing the Erlang function.
         * @return a reference to this builder.
         */
        public Builder withModule(String module)
        {
            stringCheck(module);
            switch(type)
            {
                case UNKNOWN:
                case ERLANG:
                    this.module = module;
                    type = Type.ERLANG;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a module.");
            }
            return this;
        }
        
        /**
         * Set the function name for an Erlang function.
         * @param function the name of the Erlang function.
         * @return a reference to this builder.
         */
        public Builder withFunction(String function)
        {
            stringCheck(function);
            switch(type)
            {
                case UNKNOWN:
                case ERLANG:
                    this.function = function;
                    type = Type.ERLANG;
                    break;
                default:
                    throw new IllegalArgumentException(type + " can not have a function.");
            }
            return this;
        }
        
        private void stringCheck(String arg)
        {
            if (null == arg || arg.length() == 0)
            {
                throw new IllegalArgumentException("String can not be null or zero length.");
            }
        }
        
        /**
         * Construct and return a Function.
         * @return a Function 
         */
        public Function build()
        {
            switch(type)
            {
                case UNKNOWN:
                    throw new IllegalStateException("Nothing to build.");
                case STORED_JS:
                    if (null == bucket || null == key)
                    {
                        throw new IllegalArgumentException("Stored Javascript requires both a bucket and key");
                    }
                    break;
                case ERLANG:
                    if (null == module || null == function)
                    {
                        throw new IllegalArgumentException("Erlang requires both a module and a function");
                    }
                    break;
                default:
                    break;
            }
            
            return new Function(this);
        }
    }
    
}
