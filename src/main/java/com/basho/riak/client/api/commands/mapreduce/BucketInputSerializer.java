package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Namespace;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class BucketInputSerializer extends JsonSerializer<BucketInput>
{
	@Override
	public void serialize(BucketInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		if (input.hasFilters())
        {
            jg.writeStartObject();
            if (!input.getNamespace().getBucketType().toStringUtf8().equals(Namespace.DEFAULT_BUCKET_TYPE))
            {
                jg.writeArrayFieldStart("bucket");
                jg.writeString(input.getNamespace().getBucketTypeAsString());
                jg.writeString(input.getNamespace().getBucketNameAsString());
                jg.writeEndArray();
            }
            else
            {
                jg.writeObjectField("bucket", input.getNamespace().getBucketNameAsString());
            }
            
            jg.writeObjectField("key_filters", input.getFilters());
            
            jg.writeEndObject();
        }
        else if (!input.getNamespace().getBucketType().toStringUtf8().equals(Namespace.DEFAULT_BUCKET_TYPE))
        {
            jg.writeStartArray();
            jg.writeString(input.getNamespace().getBucketTypeAsString());
            jg.writeString(input.getNamespace().getBucketNameAsString());
            jg.writeEndArray();
        }
        else
        {
            jg.writeString(input.getNamespace().getBucketNameAsString());
        }

		
	}
}
