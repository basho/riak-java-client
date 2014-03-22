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
package com.basho.riak.client.operations;

import com.basho.riak.client.util.BinaryValue;

import java.nio.ByteBuffer;

 /*
 * @author Dave Rusek <drusuk at basho dot com>
 * @since 2.0
 */
public abstract class Index<T>
{

    public enum Type
    {
        INT("_int"), BIN("_bin");

        private final String ext;

        Type(String ext)
        {
            this.ext = ext;
        }

        String getExt()
        {
            return ext;
        }
    }

    private final String name;
    private final Type type;

    protected Index(Type type, String name)
    {
        this.type = type;
        this.name = name;
    }

    abstract T convert(BinaryValue input);

    public String getName()
    {
        return name;
    }

    public String getFullName()
    {
        return name + type.getExt();
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }

        if (!(obj instanceof Index))
        {
            return false;
        }

        Index other = (Index) obj;

        return other.getType().equals(getType()) &&
            other.getName().equals(getName());

    }

    @Override
    public int hashCode()
    {
        int result = 17;
        result = 37 * result + getType().hashCode();
        result = 37 * result + getName().hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "{name: " + name + type.getExt() + "}";
    }

    public static Index<byte[]> binIndex(String name)
    {
        return new BinaryIndex(name);
    }

    public static Index<Integer> intIndex(String name)
    {
        return new IntegerIndex(name);
    }

    private static class BinaryIndex extends Index<byte[]>
    {

        public BinaryIndex(String name)
        {
            super(Type.BIN, name);
        }

        @Override
        byte[] convert(BinaryValue input)
        {
            return input.unsafeGetValue();
        }
    }

    private static class IntegerIndex extends Index<Integer>
    {

        public IntegerIndex(String name)
        {
            super(Type.INT, name);
        }

        @Override
        Integer convert(BinaryValue input)
        {
            return ByteBuffer.wrap(input.unsafeGetValue()).getInt();
        }
    }

}
