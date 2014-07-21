package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Location;

import java.util.Collection;

public class BucketKeyInput implements MapReduceInput
{

	private final Collection<IndividualInput> inputs;

	public BucketKeyInput(Collection<IndividualInput> inputs)
	{
		this.inputs = inputs;
	}

	public Collection<IndividualInput> getInputs()
	{
		return inputs;
	}

	static class IndividualInput
	{
		public final Location location;
		public final String keyData;

	  IndividualInput(Location location, String keyData)
		{
			this.location = location;
            if (keyData == null)
            {
                throw new IllegalArgumentException("keyData cannot be null.");
            }
			this.keyData = keyData;
		}

	  IndividualInput(Location location)
		{
			this.location = location;
			this.keyData = "";
		}

		public boolean hasKeyData()
		{
			return keyData != null;
		}
	}

}
