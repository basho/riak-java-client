package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Serializes a {@link BucketKeyInput} to a Riak MapReduce JSON format.
 */
public class BucketKeyInputSerializer extends JsonSerializer<BucketKeyInput>
{
    @Override
    public void serialize(BucketKeyInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
    {
        jg.writeStartArray();

        for (BucketKeyInput.IndividualInput i : input.getInputs())
        {
            jg.writeStartArray();

            Location loc = i.location;
            jg.writeString(loc.getNamespace().getBucketNameAsString());
            jg.writeString(loc.getKeyAsString());
            jg.writeObject(i.keyData);

          // TODO: Remove this when bug in Riak is fixed.
          // There's a bug in Riak where if you explicitly specify
            // "default" with the 4 argument version of input, it
            // blows up.
            if (!loc.getNamespace().getBucketTypeAsString().equals(Namespace.DEFAULT_BUCKET_TYPE))
            {
                jg.writeString(loc.getNamespace().getBucketTypeAsString());
            }
            jg.writeEndArray();
        }

        jg.writeEndArray();
    }
}
