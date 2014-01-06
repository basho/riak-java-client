/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.operations;

import com.basho.riak.client.util.BinaryValue;

public class Location
{

	private static final String DEFAULT_TYPE = "default";

	private BinaryValue type;
	private BinaryValue bucket;
	private BinaryValue key;

	/**
	 * Construct a location using both a bucket and a key
	 *
	 * @param bucket the bucket for this location
	 * @param key    the key for this location
	 */
	public Location(String bucket, String key)
	{
		this.bucket = BinaryValue.create(bucket);
		this.key = BinaryValue.create(key);
	}

	/**
	 * Construct a location using both a bucket and a key
	 *
	 * @param bucket the bucket for this location
	 * @param key    the key for this location
	 */
	public Location(BinaryValue bucket, BinaryValue key)
	{
		this.bucket = bucket;
		this.key = key;
	}

	/**
	 * Construct a location using just a bucket
	 *
	 * @param bucket the bucket for this location
	 */
	public Location(String bucket)
	{
		this.bucket = BinaryValue.create(bucket);
	}

	/**
	 * Construct a location using just a bucket
	 *
	 * @param bucket the bucket for this location
	 */
	public Location(BinaryValue bucket)
	{
		this.bucket = bucket;
	}

	/**
	 * Specify the bucket type of this location
	 *
	 * @param type the bucket type for this location
	 * @return this
	 */
	public Location withType(String type)
	{
		this.type = BinaryValue.create(type);
		return this;
	}

	/**
	 * Specify the bucket type of this location
	 *
	 * @param type the bucket type for this location
	 * @return this
	 */
	public Location withType(BinaryValue type)
	{
		this.type = type;
		return this;
	}

	/**
	 * Get the bucket type
	 *
	 * @return bucket type
	 */
	public BinaryValue getType()
	{
		return type;
	}

	/**
	 * Get the bucket
	 *
	 * @return the bucket
	 */
	public BinaryValue getBucket()
	{
		return bucket;
	}

	/**
	 * Get the key
	 *
	 * @return the key
	 */
	public BinaryValue getKey()
	{
		return key;
	}

	/**
	 * Does this location specify a type
	 *
	 * @return true if a type was specified
	 */
	public boolean hasType()
	{
		return type != null;
	}

	/**
	 * Does this location specify a key
	 *
	 * @return true if a key was specified
	 */
	public boolean hasKey()
	{
		return key != null;
	}

	@Override
	public int hashCode()
	{
		int result = 17;
		result = 37 * result + getType().hashCode();
		result = 37 * result + getBucket().hashCode();
		result = 37 * result + getKey().hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == this)
		{
			return true;
		}

		if (!(obj instanceof Location))
		{
			return false;
		}

		Location other = (Location) obj;

		return ((!hasType() && !other.hasType()) || (hasType() && other.hasType() && getType().equals(other.getType()))) &&
			( (getBucket().equals(other.getBucket()))) &&
			((!hasKey() && !other.hasKey()) || (hasKey() && other.hasKey() && getKey().equals(other.getKey())));
	}

	@Override
	public String toString()
	{
		return "{type: " + type + ", bucket: " + bucket + ", key: " + key + "}";
	}
}
