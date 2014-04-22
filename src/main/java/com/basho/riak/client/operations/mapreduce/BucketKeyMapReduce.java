package com.basho.riak.client.operations.mapreduce;

import com.basho.riak.client.query.Location;

import java.util.ArrayList;
import java.util.List;

/**
 * Perform a map-reduce operation on a list of bucket/keys
 * <p/>
 * Usage:
 * <pre>
 *   {@code
 *   Client client = ...
 *   BucketKeyMapReduce mr = new BucketKeyMapReduce.Builder()
 *     .withLocation(new Location("bucket").setKey("key1"))
 *     .withLocation(new Location("bucket2").setKey("key2"))
 *     .withLocation(new Location("bucket2").setKey("key3"), "key_data")
 *     .build();
 *   MapReduce.Response response = client.execute(mr);
 *   }
 * </pre>
 */
public class BucketKeyMapReduce extends MapReduce
{

	public BucketKeyMapReduce(BucketKeyInput input, Builder builder)
	{
		super(input, builder);
	}

	public static class Builder extends MapReduce.Builder<Builder>
	{

		private List<BucketKeyInput.IndividualInput> input =
				new ArrayList<BucketKeyInput.IndividualInput>();

		@Override
		protected Builder self()
		{
			return this;
		}

		/**
		 * Add a location to the list of locations to use as MR input.
		 *
		 * @param location a location
		 * @return this
		 */
		public Builder withLocation(Location location)
		{
			input.add(new BucketKeyInput.IndividualInput(location));
			return this;
		}

		/**
		 * Add a location to the list of locations to use as MR input along with extra input data.
		 *
		 * @param location a location
		 * @param keyData  extra data to be passed as part of the input
		 * @return this
		 */
		public Builder withLocation(Location location, String keyData)
		{
			input.add(new BucketKeyInput.IndividualInput(location, keyData));
			return this;
		}

		/**
		 * Create a new BucketKeyMapReduce request
		 *
		 * @return new reuqest
		 */
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
