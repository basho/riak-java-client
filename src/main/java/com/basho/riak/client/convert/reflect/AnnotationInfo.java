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
package com.basho.riak.client.convert.reflect;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.convert.UsermetaField;
import com.basho.riak.client.query.indexes.LongIntIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.indexes.StringBinIndex;
import com.basho.riak.client.query.links.RiakLink;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static com.basho.riak.client.convert.reflect.ClassUtil.*;

/**
 * Class that contains the Riak annotated fields for an annotated class
 *
 * @author russell
 */
public class AnnotationInfo
{

	private static final String NO_RIAK_KEY_FIELD_PRESENT = "no riak key field present";
	private static final String NO_RIAK_VCLOCK_FIELD_PRESENT = "no riak vclock field present";
	private static final String NO_RIAK_TOMBSTONE_FIELD_PRESENT = "no riak tombstone field present";
	private final Field riakKeyField;
	private final Method riakKeySetterMethod;
	private final Method riakKeyGetterMethod;
	private final List<UsermetaField> usermetaItemFields;
	private final Field usermetaMapField;
	private final List<RiakIndexField> indexFields;
	private final List<RiakIndexMethod> indexMethods;
	private final Field riakLinksField;
	private final Field riakVClockField;
	private final Field riakTombstoneField;

	/**
	 * @param riakKeyField
	 * @param usermetaItemFields
	 * @param usermetaMapField
	 * @param riakLinksField
	 * @param indexFields
	 */

	public AnnotationInfo(Field riakKeyField, Method riakKeyGetterMethod,
	                      Method riakKeySetterMethod, List<UsermetaField> usermetaItemFields,
	                      Field usermetaMapField, List<RiakIndexField> indexFields,
	                      List<RiakIndexMethod> indexMethods, Field riakLinksField,
	                      Field riakVClockField, Field riakTombstoneField)
	{

		this.riakKeyField = riakKeyField;
		this.riakKeyGetterMethod = riakKeyGetterMethod;
		this.riakKeySetterMethod = riakKeySetterMethod;
		this.usermetaItemFields = usermetaItemFields;
		validateUsermetaMapField(usermetaMapField);
		this.usermetaMapField = usermetaMapField;
		this.indexFields = indexFields;
		this.indexMethods = indexMethods;
		validateRiakLinksField(riakLinksField);
		this.riakLinksField = riakLinksField;
		this.riakVClockField = riakVClockField;
		this.riakTombstoneField = riakTombstoneField;
	}

	/**
	 * @param riakLinksField
	 */
	private void validateRiakLinksField(Field riakLinksField)
	{
		if (riakLinksField == null)
		{
			return;
		}

		ParameterizedType type = (ParameterizedType) riakLinksField.getGenericType();
		if (type.getRawType().equals(Collection.class))
		{

			Type[] genericParams = type.getActualTypeArguments();
			if (genericParams.length == 1 && genericParams[0].equals(RiakLink.class))
			{
				return;
			}
		}
		throw new IllegalArgumentException("riak links field must be Collection<RiakLink>");
	}

	/**
	 * @param usermetaMapField
	 */
	private void validateUsermetaMapField(Field usermetaMapField)
	{
		if (usermetaMapField == null)
		{
			return;
		}

		ParameterizedType type = (ParameterizedType) usermetaMapField.getGenericType();
		if (type.getRawType().equals(Map.class))
		{

			Type[] genericParams = type.getActualTypeArguments();
			if (genericParams.length == 2 && genericParams[0].equals(String.class) &&
				genericParams[1].equals(String.class))
			{
				return;
			}
		}
		throw new IllegalArgumentException("user meta map field must be Map<String, String>");
	}

	/**
	 * @param <T>
	 * @param obj
	 * @return
	 */
	public <T> String getRiakKey(T obj)
	{

		Object key = null;
		if (riakKeyField != null)
		{
			key = getFieldValue(riakKeyField, obj);
		} else if (riakKeyGetterMethod != null)
		{
			key = getMethodValue(riakKeyGetterMethod, obj);
		}
		return key == null ? null : key.toString();
	}

	public <T> void setRiakKey(T obj, String key)
	{
		if (riakKeyField != null)
		{
			setFieldValue(riakKeyField, obj, key);
		} else if (riakKeySetterMethod != null)
		{
			setMethodValue(riakKeySetterMethod, obj, key);
		}
	}

	public boolean hasRiakVClock()
	{
		return riakVClockField != null;
	}

	public <T> VClock getRiakVClock(T obj)
	{
		if (!hasRiakVClock())
		{
			throw new IllegalStateException(NO_RIAK_VCLOCK_FIELD_PRESENT);
		}

		VClock vclock;

		// We allow the annotated field to be either an actual VClock, or
		// a byte array. This is enforced in the AnnotationScanner

		if (riakVClockField.getType().isAssignableFrom(VClock.class))
		{
			vclock = (VClock) getFieldValue(riakVClockField, obj);
		} else
		{
			vclock = new BasicVClock((byte[]) getFieldValue(riakVClockField, obj));
		}

		return vclock;

	}

	public <T> void setRiakVClock(T obj, VClock vclock)
	{
		if (!hasRiakVClock())
		{
			throw new IllegalStateException(NO_RIAK_VCLOCK_FIELD_PRESENT);
		}

		// We allow the annotated field to be either an actual VClock, or
		// a byte array. This is enforced in the AnnotationScanner

		if (riakVClockField.getType().isAssignableFrom(VClock.class))
		{
			setFieldValue(riakVClockField, obj, vclock);
		} else
		{
			setFieldValue(riakVClockField, obj, vclock.getBytes());
		}
	}

	public boolean hasRiakTombstone()
	{
		return riakTombstoneField != null;
	}

	public <T> boolean getRiakTombstone(T obj)
	{
		if (!hasRiakTombstone())
		{
			throw new IllegalStateException(NO_RIAK_TOMBSTONE_FIELD_PRESENT);
		}

		boolean tombstone = (Boolean) getFieldValue(riakTombstoneField, obj);
		return tombstone;
	}

	public <T> void setRiakTombstone(T obj, Boolean isDeleted)
	{
		if (!hasRiakTombstone())
		{
			throw new IllegalStateException(NO_RIAK_TOMBSTONE_FIELD_PRESENT);
		}

		setFieldValue(riakTombstoneField, obj, isDeleted);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> Map<String, String> getUsermetaData(T obj)
	{
		final Map<String, String> usermetaData = new LinkedHashMap<String, String>();
		Map<String, String> objectMetaMap = null;

		for (UsermetaField f : usermetaItemFields)
		{
			Object o = getFieldValue(f.getField(), obj);
			String val = o == null ? null : o.toString();
			String key = f.getUsermetaDataKey();
			// null is not a user meta datum
			if (o != null)
			{
				usermetaData.put(key, val);
			}
		}

		if (usermetaMapField != null)
		{
			objectMetaMap = (Map) getFieldValue(usermetaMapField, obj);
		}

		if (objectMetaMap != null)
		{
			usermetaData.putAll(objectMetaMap);
		}
		return usermetaData;
	}

	public <T> void setUsermetaData(final Map<String, String> usermetaData, T obj)
	{
		// copy as we will modify
		final Map<String, String> localMetaCopy = new HashMap<String, String>(usermetaData);

		// set any individual annotated fields
		for (UsermetaField f : usermetaItemFields)
		{
			if (localMetaCopy.containsKey(f.getUsermetaDataKey()))
			{
				setFieldValue(f.getField(), obj, localMetaCopy.get(f.getUsermetaDataKey()));
				localMetaCopy.remove(f.getUsermetaDataKey());
			}
		}

		// set a catch all map field
		if (usermetaMapField != null)
		{
			setFieldValue(usermetaMapField, obj, localMetaCopy);
		}
	}

	/**
	 * @return a {@link RiakIndexes} made of the values of the RiakIndex annotated fields and methods. For methods it is
	 * expected to be a Set&lt;Long&gt; or Set&lt;String&gt;
	 */
	@SuppressWarnings("unchecked")
	public <T> RiakIndexes getIndexes(T obj)
	{
		final RiakIndexes riakIndexes = new RiakIndexes();

		for (RiakIndexField f : indexFields)
		{
			if (Set.class.isAssignableFrom(f.getType()))
			{
				final Type t = f.getField().getGenericType();
				if (t instanceof ParameterizedType)
				{
					Class genericType = (Class) ((ParameterizedType) t).getActualTypeArguments()[0];
					if (String.class.equals(genericType))
					{
						riakIndexes.getIndex(new StringBinIndex.Name(f.getIndexName()))
							.add((Set<String>) getFieldValue(f.getField(), obj));
					} else if (Long.class.equals(genericType) || Integer.class.equals(genericType))
					{
						riakIndexes.getIndex(new LongIntIndex.Name(f.getIndexName()))
							.add((Set<Long>) getFieldValue(f.getField(), obj));
					} else if (Integer.class.equals(genericType))
					{
						// Supporting Integer as legacy. All new code should use Long
						Set<Integer> iSet = (Set<Integer>) getFieldValue(f.getField(), obj);
						Set<Long> lSet = new HashSet<Long>();
						for (Integer i : iSet)
						{
							lSet.add(i.longValue());
						}
						riakIndexes.getIndex(new LongIntIndex.Name(f.getIndexName())).add(lSet);
					}
				}
			} else
			{
				final Object val = getFieldValue(f.getField(), obj);
				// null is not an index value
				if (val != null)
				{
					if (val instanceof String)
					{
						riakIndexes.getIndex(new StringBinIndex.Name(f.getIndexName())).add((String) val);
					} else if (val instanceof Long)
					{
						riakIndexes.getIndex(new LongIntIndex.Name(f.getIndexName())).add((Long) val);
					} else if (val instanceof Integer)
					{
						// Supporting int / Integer for legacy. New code should use long / Long
						riakIndexes.getIndex(new LongIntIndex.Name(f.getIndexName())).add(((Integer) val).longValue());
					}
				}
			}
		}

		for (RiakIndexMethod m : indexMethods)
		{
			if (Set.class.isAssignableFrom(m.getType()))
			{
				final Type t = m.getMethod().getGenericReturnType();
				if (t instanceof ParameterizedType)
				{
					final Object val = getMethodValue(m.getMethod(), obj);
					if (val != null)
					{
						final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
						if (String.class.equals(genericType))
						{
							riakIndexes.getIndex(new StringBinIndex.Name(m.getIndexName())).add((Set<String>) val);
						} else if (Long.class.equals(genericType))
						{
							riakIndexes.getIndex(new LongIntIndex.Name(m.getIndexName())).add((Set<Long>) val);
						} else if (Integer.class.equals(genericType))
						{
							// Supporting Integer as legacy. All new code should use Long
							Set<Integer> iSet = (Set<Integer>) val;
							Set<Long> lSet = new HashSet<Long>();
							for (Integer i : iSet)
							{
								lSet.add(i.longValue());
							}
							riakIndexes.getIndex(new LongIntIndex.Name(m.getIndexName())).add(lSet);
						}
					}
				}
			} else
			{
				final Object val = getMethodValue(m.getMethod(), obj);
				// null is not an index value
				if (val != null)
				{
					if (val instanceof String)
					{
						riakIndexes.getIndex(new StringBinIndex.Name(m.getIndexName())).add((String) val);
					} else if (val instanceof Long)
					{
						riakIndexes.getIndex(new LongIntIndex.Name(m.getIndexName())).add((Long) val);
					} else if (val instanceof Integer)
					{
						riakIndexes.getIndex(new LongIntIndex.Name(m.getIndexName())).add(((Integer) val).longValue());
					}
				}
			}
		}

		return riakIndexes;
	}

	/**
	 * @param <T>
	 * @param indexes the RiakIndexes to copy to the domain object
	 * @param obj     the domain object to set indexes on
	 */
	public <T> void setIndexes(RiakIndexes indexes, T obj)
	{
		// copy the index values to the correct fields
		for (RiakIndexField f : indexFields)
		{
			Set<?> val = null;

			if (Set.class.isAssignableFrom(f.getType()))
			{
				final Type t = f.getField().getGenericType();
				if (t instanceof ParameterizedType)
				{
					final Class<?> genericType = (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
					if (String.class.equals(genericType))
					{
						val = indexes.getIndex(new StringBinIndex.Name(f.getIndexName())).values();
					} else if (Integer.class.equals(genericType))
					{
						val = indexes.getIndex(new LongIntIndex.Name(f.getIndexName())).values();
					}
				}
				if (val != null && !val.isEmpty())
				{
					setFieldValue(f.getField(), obj, val);
				}
			} else
			{
				if (Integer.class.equals(f.getType()) || int.class.equals(f.getType()))
				{
					// Support Integer / int for legacy. New code should use Long / long
					Set<Long> lSet = indexes.getIndex(new LongIntIndex.Name(f.getIndexName())).values();
					Set<Integer> iSet = new HashSet<Integer>();
					for (Long l : lSet)
					{
						iSet.add(l.intValue());
					}
					val = iSet;
				} else if (String.class.equals(f.getType()))
				{
					val = indexes.getIndex(new StringBinIndex.Name(f.getIndexName())).values();
				} else if (Long.class.equals(f.getType()) || long.class.equals(f.getType()))
				{
					val = indexes.getIndex(new LongIntIndex.Name(f.getIndexName())).values();
				}

				if (val != null && !val.isEmpty())
				{
					setFieldValue(f.getField(), obj, val.iterator().next()); // take the first value
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <T> List<RiakLink> getLinks(T obj)
	{
		final ArrayList<RiakLink> links = new ArrayList<RiakLink>();
		if (riakLinksField != null)
		{
			Object o = getFieldValue(riakLinksField, obj);
			if (o != null && o instanceof Collection)
			{
				links.addAll((Collection<RiakLink>) o);
			}
		}
		return links;
	}

	public <T> void setLinks(Collection<RiakLink> links, T obj)
	{
		if (riakLinksField != null)
		{
			setFieldValue(riakLinksField, obj, links);
		}
	}
}
