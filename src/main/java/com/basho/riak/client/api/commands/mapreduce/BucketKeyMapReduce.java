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
    /**
     * Creates a new BucketKeyMapReduce operation from the supplied configuration.
     * @param input the input to use for the BucketKeyMapReduce input phase.
     * @param builder the builder to use for the BucketKeyMapReduce input phase.
     */
    public BucketKeyMapReduce(BucketKeyInput input, Builder builder)
    {
        super(input, builder);
    }

    /**
     * Builder for a BucketKeyMapReduce command.
     */
    public static class Builder extends MapReduce.Builder<Builder>
    {
        private List<BucketKeyInput.IndividualInput> input = new ArrayList<>();

        @Override
        protected Builder self()
        {
            return this;
        }

        /**
         * Adds a new location to the collection of MapReduce inputs.
         * @param location the location to add to the inputs.
         * @return a reference to this object.
         */
        public Builder withLocation(Location location)
        {
            input.add(new BucketKeyInput.IndividualInput(location));
            return this;
        }

        /**
         * Adds a new location + key to the collection of MapReduce inputs.
         * @param location the location to add to the inputs.
         * @param keyData metadata which will be passed as an argument to a map function
         *                when evaluated on the object stored under the provided location.
         * @return a reference to this object.
         */
        public Builder withLocation(Location location, String keyData)
        {
            input.add(new BucketKeyInput.IndividualInput(location, keyData));
            return this;
        }

        /**
         * Construct a new BucketKeyMapReduce operation.
         * @return the new BucketKeyMapReduce operation.
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
