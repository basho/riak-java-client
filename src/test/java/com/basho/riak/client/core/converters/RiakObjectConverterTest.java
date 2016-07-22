package com.basho.riak.client.core.converters;

import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class RiakObjectConverterTest
{

    public static final String APPLICATION_JSON = "application/json";
    public static final String UTF_8 = "UTF-8";

    @Test
    public void contentTypeAndCharsetAreEncodedProperly()
    {
        RiakObject riakObject = new RiakObject();
        riakObject.setContentType(APPLICATION_JSON);
        riakObject.setCharset(UTF_8);
        riakObject.setValue(BinaryValue.create("foo"));

        final RiakKvPB.RpbContent pbObject = RiakObjectConverter.convert(riakObject);
        assertTrue(pbObject.hasCharset());
        assertTrue(pbObject.hasContentType());

        assertEquals(UTF_8, pbObject.getCharset().toStringUtf8());
        assertEquals(APPLICATION_JSON, pbObject.getContentType().toStringUtf8());
    }

    @Test
    public void contentTypeAndCharsetAreDecodedProperly()
    {
        final RiakKvPB.RpbContent pbObject = RiakKvPB.RpbContent.newBuilder()
                                                            .setValue(ByteString.copyFromUtf8("foo"))
                                                            .setContentType(ByteString.copyFromUtf8(APPLICATION_JSON))
                                                            .setCharset(ByteString.copyFromUtf8(UTF_8)).build();

        final List<RiakObject> riakObjects = RiakObjectConverter.convert(
                new ArrayList<RiakKvPB.RpbContent>() {{ add(pbObject); }}, ByteString.EMPTY);

        assertEquals(1, riakObjects.size());
        final RiakObject riakObject = riakObjects.get(0);
        final String charset = riakObject.getCharset();
        final String contentType = riakObject.getContentType();

        assertTrue(riakObject.hasCharset());
        assertTrue(contentType != null && !contentType.isEmpty());

        assertEquals(UTF_8, charset);
        assertEquals(APPLICATION_JSON, contentType);
    }
}
