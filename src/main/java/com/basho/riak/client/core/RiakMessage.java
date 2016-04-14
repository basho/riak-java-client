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
package com.basho.riak.client.core;

/**
 * Encapsulates the raw bytes sent to or received from Riak.
 * @author Brian Roach <roach at basho dot com>
 * @author Sergey Galkin <sgalkin at basho dot com>
 * @since 2.0
 */
public final class RiakMessage
{
    private final byte code;
    private byte[] data;
    private final Object dataObject;

    public RiakMessage(byte code, byte[] data)
    {
        this.code = code;
        this.data = data;
        this.dataObject = null;
    }

    /**
     * Creates message for deferred encoding.
     *
     * @author Sergey Galkin <sgalkin at basho dot com>
     * @since 2.4
     */
    public RiakMessage(byte code, Object dataObj)
    {
        this.code = code;
        this.data = null;
        this.dataObject = dataObj;
    }

    public byte getCode()
    {
        return code;
    }

    public byte[] getData()
    {
        return data;
    }

    /**
     * @author Sergey Galkin <sgalkin at basho dot com>
     * @since 2.4
     */
    public boolean isEncoded()
    {
        return data != null;
    }

    /**
     * @author Sergey Galkin <sgalkin at basho dot com>
     * @since 2.4
     */
    public void setData(byte[] data)
    {
        this.data = data;
    }

    /**
     * @author Sergey Galkin <sgalkin at basho dot com>
     * @since 2.4
     */
    @SuppressWarnings("unchecked")
    public <T> T getDataObject()
    {
        return (T) dataObject;
    }
}
