package com.basho.riak.client.api.commands.mapreduce;

import com.basho.riak.client.core.query.Location;

import java.util.Collection;

/**
 * MapReduce Input that provides a specific set of {@link Location}s
 * for a MapReduce job.
 */
public class BucketKeyInput implements MapReduceInput
{
    private final Collection<IndividualInput> inputs;

    /**
     * Create a new BucketKeyInput with the supplied collection of IndividualInputs.
     * @param inputs the IndividualInputs to use for the MapReduce input.
     */
    public BucketKeyInput(Collection<IndividualInput> inputs)
    {
        this.inputs = inputs;
    }

    /**
     * Gets the collection of {@link com.basho.riak.client.api.commands.mapreduce.BucketKeyInput.IndividualInput}s.
     * @return the collection of {@link com.basho.riak.client.api.commands.mapreduce.BucketKeyInput.IndividualInput}s
     */
    public Collection<IndividualInput> getInputs()
    {
        return inputs;
    }

    /**
     * Represents a BucketType/Bucket/Key combination, for use with the MapReduce BucketKeyInput.
     */
    static class IndividualInput
    {
        public final Location location;
        public final String keyData;

        /**
         * Creates an IndividualInput with the provided location and keyData.
         * @param location the location of the IndividualInput
         * @param keyData metadata which will be passed as an argument to a map function
         *                when evaluated on the object stored under the provided location
         */
        IndividualInput(Location location, String keyData)
        {
            this.location = location;
            if (keyData == null)
            {
                throw new IllegalArgumentException("keyData cannot be null.");
            }
            this.keyData = keyData;
        }

        /**
         * Creates an IndividualInput with the provided location.
         * @param location the location of the IndividualInput
         */
        IndividualInput(Location location)
        {
            this.location = location;
            this.keyData = "";
        }

        /**
         * Indicates whether this IndividualInput has keyData metadata.
         * @return true if keyData was passed in during creation
         */
        public boolean hasKeyData()
        {
            return keyData != null && keyData != "";
        }
    }
}
