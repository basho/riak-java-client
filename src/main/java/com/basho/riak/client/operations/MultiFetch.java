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

import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.core.RiakCluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.unmodifiableList;

public class MultiFetch<T> extends RiakCommand<MultiFetch.Response>
{

	private final Converter<T> converter;
	private final ArrayList<Location> keys = new ArrayList<Location>();

	private MultiFetch(Builder builder)
	{
		this.converter = builder.converter;
		this.keys.addAll(builder.keys);
	}

	@Override
	Response<T> execute(RiakCluster cluster) throws ExecutionException, InterruptedException
	{

		List<FetchValue.Response<T>> values = new ArrayList<FetchValue.Response<T>>();
		for (Location key : keys)
		{
			values.add(new FetchValue.Builder<T>(key).withConverter(converter).build().execute(cluster));
		}

		return new Response<T>(values);

	}

	public static class Builder<T>
	{

		private Converter<T> converter;
		private ArrayList<Location> keys = new ArrayList<Location>();

		public Builder withConverter(Converter<T> converter)
		{
			this.converter = converter;
			return this;
		}

		public Builder withKey(Location key)
		{
			keys.add(key);
			return this;
		}

		public Builder withKeys(Location... key)
		{
			keys.addAll(Arrays.asList(key));
			return this;
		}

		public Builder withKeys(Iterable<Location> key)
		{
			for (Location loc : key)
			{
				keys.add(loc);
			}
			return this;
		}

		public MultiFetch<T> build()
		{
			return new MultiFetch<T>(this);
		}

	}

    public static final class Response<T> implements Iterable<FetchValue.Response<T>>
    {

        private final List<FetchValue.Response<T>> responses;

        Response(List<FetchValue.Response<T>> responses)
        {
            this.responses = responses;
        }

        @Override
        public Iterator<FetchValue.Response<T>> iterator()
        {
            return unmodifiableList(responses).iterator();
        }
    }

}
