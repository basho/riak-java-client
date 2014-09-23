package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Location;

import java.util.ArrayList;
import java.util.List;

/**
 * Command used to perform a map reduce operation over a specific set of keys in a bucket.
 * @author Dave Rusek <drusek at basho dot com>
 * @since 2.0
 */
public class BucketKeyMapReduce extends MapReduce
{

	public BucketKeyMapReduce(BucketKeyInput input, Builder builder)
	{
		super(input, builder);
	}

    /**
     * Builder for a BucketKeyMapReduce command.
     */
	public static class Builder extends MapReduce.Builder<Builder>
	{

		private List<BucketKeyInput.IndividualInput> input =
				new ArrayList<BucketKeyInput.IndividualInput>();

		@Override
		protected Builder self()
		{
			return this;
		}

		public Builder withLocation(Location location)
		{
			input.add(new BucketKeyInput.IndividualInput(location));
			return this;
		}

		public Builder withLocation(Location location, String keyData)
		{
			input.add(new BucketKeyInput.IndividualInput(location, keyData));
			return this;
		}

		public BucketKeyMapReduce build()
		{
			if (input == null)
			{
				throw new IllegalStateException("At least one location must be specified");
			}

			return new BucketKeyMapReduce(new BucketKeyInput(input), this);
		}

	}

}
