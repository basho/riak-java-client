package com.basho.riak.client.operations.mapreduce;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class IndexInputSerializer extends JsonSerializer<IndexInput>
{
	@Override
	public void serialize(IndexInput input, JsonGenerator jg, SerializerProvider sp) throws IOException
	{
		jg.writeStartObject();
		jg.writeStringField("bucket", input.getBucket().getBucketNameAsString());
		jg.writeStringField("index", input.getIndex());
		IndexInput.IndexCriteria criteria = input.getCriteria();
		if (criteria instanceof IndexInput.MatchCriteria)
		{
			IndexInput.MatchCriteria<?> match = (IndexInput.MatchCriteria) criteria;
			jg.writeObjectField("key", match.getValue());
		} else if (criteria instanceof IndexInput.RangeCriteria)
		{
			IndexInput.RangeCriteria range = (IndexInput.RangeCriteria) criteria;
			jg.writeObjectField("start", range.getBegin());
			jg.writeObjectField("end", range.getEnd());
		}
		jg.writeEndObject();
	}
}
