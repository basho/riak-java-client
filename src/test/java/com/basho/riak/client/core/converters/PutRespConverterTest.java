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
package com.basho.riak.client.core.converters;

import com.basho.riak.client.cap.BasicVClock;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.query.UserMetadata.RiakUserMetadata;
import com.basho.riak.client.query.indexes.IndexType;
import com.basho.riak.client.query.indexes.RiakIndex;
import com.basho.riak.client.query.indexes.RiakIndexes;
import com.basho.riak.client.query.links.RiakLink;
import com.basho.riak.client.query.links.RiakLinks;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PutRespConverterTest
{

	private final String keyStr = "key";
	private final String bucketStr = "bucket";

	@Test
	public void decodeLinksTest()
	{

		String tagStr = "tag";

		ByteString key = ByteString.copyFromUtf8(keyStr);
		ByteString bucket = ByteString.copyFromUtf8(bucketStr);
		ByteString tag = ByteString.copyFromUtf8(tagStr);

		RiakKvPB.RpbLink link = RiakKvPB.RpbLink.newBuilder()
			.setKey(key)
			.setBucket(bucket)
			.setTag(tag)
			.build();

		List<RiakKvPB.RpbLink> links = new ArrayList<RiakKvPB.RpbLink>();
		links.add(link);

		ByteArrayWrapper keyWrapper = ByteArrayWrapper.create(keyStr.getBytes());
		ByteArrayWrapper bucketWrapper = ByteArrayWrapper.create(bucketStr.getBytes());
		PutRespConverter converter = new PutRespConverter(keyWrapper, bucketWrapper);

		// List with 1 value
		RiakLinks riakLinks = converter.decodeLinks(links);

		assertEquals(riakLinks.size(), 1);

		RiakLink decodedLink = riakLinks.iterator().next();

		assertEquals(decodedLink.getBucket(), bucketStr);
		assertEquals(decodedLink.getKey(), keyStr);
		assertEquals(decodedLink.getTag(), tagStr);

		// Empty list
		riakLinks = converter.decodeLinks(new ArrayList<RiakKvPB.RpbLink>());

		assertTrue(riakLinks.size() == 0);

	}


	@Test
	public void decodeIndexesTest()
	{

		ByteString indexKey = ByteString.copyFromUtf8("key_bin");
		ByteString indexValue = ByteString.copyFrom(new byte[]{1});

		RiakPB.RpbPair index =
			RiakPB.RpbPair.newBuilder()
				.setKey(indexKey)
				.setValue(indexValue)
				.build();

		List<RiakPB.RpbPair> pbIndexes = new ArrayList<RiakPB.RpbPair>();
		pbIndexes.add(index);

		ByteArrayWrapper keyWrapper = ByteArrayWrapper.create(keyStr.getBytes());
		ByteArrayWrapper bucketWrapper = ByteArrayWrapper.create(bucketStr.getBytes());
		PutRespConverter converter = new PutRespConverter(keyWrapper, bucketWrapper);

		RiakIndexes riakIndexes = converter.decodeIndexes(pbIndexes);

		assertEquals(riakIndexes.size(), 1);

		RiakIndex<?> riakIndex = riakIndexes.iterator().next();

		assertEquals(riakIndex.getFullname(), indexKey.toStringUtf8());
		assertEquals(riakIndex.getType(), IndexType.BIN);

		riakIndexes = converter.decodeIndexes(new ArrayList<RiakPB.RpbPair>());

		assertEquals(riakIndexes.size(), 0);
	}

	@Test
	public void decodeUserMetadataTest()
	{

		ByteString metadataKey = ByteString.copyFromUtf8("metadata_key");
		ByteString metadataValue = ByteString.copyFromUtf8("metadata_value");

		RiakPB.RpbPair index =
			RiakPB.RpbPair.newBuilder()
				.setKey(metadataKey)
				.setValue(metadataValue)
				.build();

		List<RiakPB.RpbPair> pbMetadata = new ArrayList<RiakPB.RpbPair>();
		pbMetadata.add(index);

		ByteArrayWrapper keyWrapper = ByteArrayWrapper.create(keyStr.getBytes());
		ByteArrayWrapper bucketWrapper = ByteArrayWrapper.create(bucketStr.getBytes());
		PutRespConverter converter = new PutRespConverter(keyWrapper, bucketWrapper);

		RiakUserMetadata riakUserMetadata = converter.decodeUserMetadata(pbMetadata);

		assertEquals(riakUserMetadata.size(), 1);

		Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry =
			riakUserMetadata.getUserMetadata().iterator().next();

		assertEquals(entry.getKey(), ByteArrayWrapper.create(metadataKey.toByteArray()));
		assertEquals(entry.getValue(), ByteArrayWrapper.create(metadataValue.toByteArray()));

		riakUserMetadata = converter.decodeUserMetadata(new ArrayList<RiakPB.RpbPair>());

		assertEquals(riakUserMetadata.size(), 0);

	}

	@Test
	public void decodeResponseTest() throws InvalidProtocolBufferException
	{

		VClock vclock = new BasicVClock("this is a vclock");
		byte[] expectedValue = new byte[]{'O', '_', 'o'};
		String expectedContentType = "content-type";
		String expectedCharset = "charset";
		String expectedContentEncoding = "content-encoding";
		String expectedVtag = "vtag";

		RiakKvPB.RpbPutResp.Builder response =
			RiakKvPB.RpbPutResp.newBuilder()
				.setVclock(ByteString.copyFrom(vclock.getBytes()))
				.setKey(ByteString.copyFromUtf8(keyStr))
				.addContent(RiakKvPB.RpbContent.newBuilder()
					.setValue(ByteString.copyFrom(expectedValue))
					.setCharset(ByteString.copyFromUtf8(expectedCharset))
					.setContentEncoding(ByteString.copyFromUtf8(expectedContentEncoding))
					.setContentType(ByteString.copyFromUtf8(expectedContentType))
					.setDeleted(false)
					.setLastMod(1000000)
					.setVtag(ByteString.copyFromUtf8(expectedVtag))
					.addIndexes(RiakPB.RpbPair.newBuilder()
						.setKey(ByteString.copyFromUtf8("key_bin"))
						.setValue(ByteString.copyFromUtf8("value")))
					.addLinks(RiakKvPB.RpbLink.newBuilder()
						.setBucket(ByteString.copyFromUtf8("bucket"))
						.setKey(ByteString.copyFromUtf8("key"))
						.setTag(ByteString.copyFromUtf8("tag")))
					.addUsermeta(RiakPB.RpbPair.newBuilder()
						.setKey(ByteString.copyFromUtf8("key"))
						.setValue(ByteString.copyFromUtf8("value"))));

		ByteArrayWrapper keyWrapper = ByteArrayWrapper.create(keyStr.getBytes());
		ByteArrayWrapper bucketWrapper = ByteArrayWrapper.create(bucketStr.getBytes());
		PutRespConverter converter = spy(new PutRespConverter(bucketWrapper, keyWrapper));

		List<RiakObject> riakObjects = converter.convert(response.build());
		assertTrue(riakObjects.size() == 1);
		RiakObject riakObject = riakObjects.get(0);
		assertEquals(keyStr, riakObject.getKeyAsString());
		assertTrue(Arrays.equals(expectedValue, riakObject.getValueAsBytes()));
		assertTrue(Arrays.equals(vclock.getBytes(), riakObject.getVClock().getBytes()));
		//assertEquals(expectedContentType, riakObject.getContentType());
		assertEquals(expectedCharset, riakObject.getCharset());
		assertFalse(riakObject.isDeleted());
		assertEquals(expectedVtag, riakObject.getVtag());

		// We have other tests for these methods, make sure they are at least called.
		verify(converter, atLeastOnce()).decodeLinks(Matchers.anyList());
		verify(converter, atLeastOnce()).decodeIndexes(Matchers.anyList());
		verify(converter, atLeastOnce()).decodeUserMetadata(Matchers.anyList());

		assertTrue(riakObject.getLinks().size() == 1);
		assertTrue(riakObject.getIndexes().size() == 1);
		assertTrue(riakObject.getUserMeta().size() == 1);

	}

}
