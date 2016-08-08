package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializes a {@link BinaryValue} to a UTF-8 string for the Riak MapReduce JSON format.
 */
public class BinaryValueSerializer extends JsonSerializer<BinaryValue>
{
    @Override
    public void serialize(BinaryValue binaryValue, JsonGenerator jg, SerializerProvider serializerProvider)
            throws IOException
    {
        jg.writeString(binaryValue.toStringUtf8());
    }
}
