/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.convert;

import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.util.BinaryValue;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a RiakObject's value to an instance of T. T must have a field annotated with {@link RiakKey} or you must
 * construct the converter with a key to use. RiakObject's value *must* be a JSON string. <p/> <p> At present user meta
 * data and {@link RiakLinks}s are not converted. This means they are essentially lost in translation. </p>
 *
 * @author russell
 */
public class JSONConverter<T> implements Converter<T>
{

	// Object mapper per domain class is expensive, a singleton (and ThreadSafe) will do.
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	static
	{
		OBJECT_MAPPER.registerModule(new RiakJacksonModule());
		OBJECT_MAPPER.registerModule(new JodaModule());
	}

	private final Class<T> clazz;
	private final UsermetaConverter<T> usermetaConverter;
	private final RiakIndexConverter<T> riakIndexConverter;
	private final RiakLinksConverter<T> riakLinksConverter;

	/**
	 * Create a JSONConverter for creating instances of <code>clazz</code> from JSON and instances of {@link RiakObject}
	 * with a JSON payload from instances of <code>clazz</code>
	 *
	 * @param clazz      the type to convert to/from
	 */
	public JSONConverter(Class<T> clazz)
	{
		this.clazz = clazz;
		this.usermetaConverter = new UsermetaConverter<T>();
		this.riakIndexConverter = new RiakIndexConverter<T>();
		this.riakLinksConverter = new RiakLinksConverter<T>();
	}

	/**
	 * Converts <code>domainObject</code> to a JSON string and sets that as the payload of a {@link RiakObject}. Also
	 * set the <code>content-type</code> to <code>application/json;charset=UTF-8</code>
	 *
	 * @param domainObject to be converted
	 */
	public RiakObject fromDomain(T domainObject) throws ConversionException
	{
		try
		{

			BinaryValue value = BinaryValue.create(OBJECT_MAPPER.writeValueAsBytes(domainObject));
			RiakObject riakObject = new RiakObject()
				.setValue(value)
				.setCharset("charset=UTF-8")
				.setContentType("application/json");

			List<RiakLink> links = riakLinksConverter.getLinks(domainObject);
			riakObject.getLinks().addLinks(links);

			RiakIndexes indexes = riakIndexConverter.getIndexes(domainObject);
			for (RiakIndex index : indexes)
			{
				riakObject.getIndexes().getIndex(index.getIndexName()).add(index.values());
			}

			Map<String, String> usermetaData = usermetaConverter.getUsermetaData(domainObject);
			for (Map.Entry<String, String> entry : usermetaData.entrySet())
			{
				riakObject.getUserMeta().put(entry.getKey(), entry.getValue());
			}

			return riakObject;

		} catch (JsonProcessingException e)
		{
			throw new ConversionException(e);
		} catch (IOException e)
		{
			throw new ConversionException(e);
		}

	}

	/**
	 * Converts the <code>value</code> of <code>riakObject</code> to an instance of <code>T</code>.
	 *
	 * @param riakObject the {@link com.basho.riak.client.query.RiakObject} to convert to instance of <code>T</code>. NOTE:
	 *                   <code>riakObject.getValue()</code> must be a JSON string. The charset from
	 *                   <code>riakObject.getContentType()</code> is used.
	 * @param vClock
	 * @param key
	 */
	public T toDomain(final RiakObject riakObject, VClock vClock, BinaryValue key) throws ConversionException
	{
		if (riakObject == null)
		{
			return null;
		} else if (riakObject.isDeleted())
		{
			try
			{
				final T domainObject = clazz.newInstance();
				TombstoneUtil.setTombstone(domainObject, true);
				VClockUtil.setVClock(domainObject, vClock);
				KeyUtil.setKey(domainObject, key.toStringUtf8());
				return domainObject;
			} catch (InstantiationException ex)
			{
				throw new ConversionException("POJO does not provide no-arg constructor", ex);
			} catch (IllegalAccessException ex)
			{
				throw new ConversionException(ex);
			}
		} else
		{
			try
			{
				final T domainObject = OBJECT_MAPPER.readValue(riakObject.getValue().unsafeGetValue(), clazz);
				KeyUtil.setKey(domainObject, key.toStringUtf8());
				VClockUtil.setVClock(domainObject, vClock);

				Map<String, String> meta = new HashMap<String, String>();
				for (Map.Entry<BinaryValue, BinaryValue> entry : riakObject.getUserMeta().getUserMetadata())
				{
					meta.put(entry.getKey().toStringUtf8(), entry.getValue().toStringUtf8());
				}

				usermetaConverter.populateUsermeta(meta, domainObject);
				riakIndexConverter.populateIndexes(riakObject.getIndexes(), domainObject);

				ArrayList<RiakLink> links = new ArrayList<RiakLink>();
				for (RiakLink link : riakObject.getLinks()) {
					links.add(link);
				}
				riakLinksConverter.populateLinks(links, domainObject);
				return domainObject;
			} catch (JsonProcessingException e)
			{
				throw new ConversionException(e);
			} catch (IOException e)
			{
				throw new ConversionException(e);
			}
		}
	}

	/**
	 * Returns the {@link ObjectMapper} being used. This is a convenience method to allow changing its behavior.
	 *
	 * @return The Jackson ObjectMapper
	 */
	public static ObjectMapper getObjectMapper()
	{
		return OBJECT_MAPPER;
	}

	/**
	 * Convenient method to register a Jackson module into the singleton Object mapper used by domain objects.
	 *
	 * @param jacksonModule Module to register.
	 */
	public static void registerJacksonModule(final Module jacksonModule)
	{
		OBJECT_MAPPER.registerModule(jacksonModule);
	}

}
