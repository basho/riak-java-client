package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

class BucketInputSerializer extends JsonSerializer<BucketInput>
{
	@Override
	public void serialize(BucketInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		if (input.hasFilters())
        {
            jg.writeStartObject();
            if (!input.getLocation().getBucketType().equals(Location.DEFAULT_BUCKET_TYPE))
            {
                jg.writeArrayFieldStart("bucket");
                jg.writeString(input.getLocation().getBucketTypeAsString());
                jg.writeString(input.getLocation().getBucketNameAsString());
                jg.writeEndArray();
            }
            else
            {
                jg.writeObjectField("bucket", input.getLocation().getBucketNameAsString());
            }
            
            jg.writeObjectField("key_filters", input.getFilters());
            
            jg.writeEndObject();
        }
        else if (!input.getLocation().getBucketType().equals(Location.DEFAULT_BUCKET_TYPE))
        {
            jg.writeStartArray();
            jg.writeString(input.getLocation().getBucketTypeAsString());
            jg.writeString(input.getLocation().getBucketNameAsString());
            jg.writeEndArray();
        }
        else
        {
            jg.writeString(input.getLocation().getBucketNameAsString());
        }

		
	}
}
