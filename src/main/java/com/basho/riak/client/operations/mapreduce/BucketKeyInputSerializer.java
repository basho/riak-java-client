package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

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
		  jg.writeString(loc.getBucketNameAsString());
		  jg.writeString(loc.getKeyAsString());
		  if (i.keyData != null)
		  {
			  jg.writeObject(i.keyData);
		  }
		  jg.writeString(loc.getBucketTypeAsString());

		  jg.writeEndArray();

	  }

		jg.writeEndArray();

	}
}
