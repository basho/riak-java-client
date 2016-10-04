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
    public static final String JZON_CONTENT_TYPE = "application/jzon"; // use a non-default content type
    public static final String UTF_48_CHARSET = "UTF-48"; // use a non-default charset

    @Test
    public void contentTypeAndCharsetAreEncodedProperly()
    {
        RiakObject riakObject = new RiakObject();
        riakObject.setContentType(JZON_CONTENT_TYPE);
        riakObject.setCharset(UTF_48_CHARSET);
        riakObject.setValue(BinaryValue.create("foo"));

        final RiakKvPB.RpbContent pbObject = RiakObjectConverter.convert(riakObject);
        assertTrue(pbObject.hasCharset());
        assertTrue(pbObject.hasContentType());

        assertEquals(UTF_48_CHARSET, pbObject.getCharset().toStringUtf8());
        assertEquals(JZON_CONTENT_TYPE, pbObject.getContentType().toStringUtf8());
    }

    @Test
    public void contentTypeAndCharsetAreDecodedProperly()
    {
        final RiakKvPB.RpbContent pbObject = RiakKvPB.RpbContent.newBuilder()
                                                                .setValue(ByteString.copyFromUtf8("foo"))
                                                                .setContentType(ByteString.copyFromUtf8(
                                                                        JZON_CONTENT_TYPE))
                                                                .setCharset(ByteString.copyFromUtf8(UTF_48_CHARSET)).build();

        final List<RiakObject> riakObjects = RiakObjectConverter.convert(
                new ArrayList<RiakKvPB.RpbContent>() {{ add(pbObject); }}, ByteString.EMPTY);

        assertEquals(1, riakObjects.size());
        final RiakObject riakObject = riakObjects.get(0);
        final String charset = riakObject.getCharset();
        final String contentType = riakObject.getContentType();

        assertTrue(riakObject.hasCharset());
        assertTrue(contentType != null && !contentType.isEmpty());

        assertEquals(UTF_48_CHARSET, charset);
        assertEquals(JZON_CONTENT_TYPE, contentType);
    }

    @Test
    public void literalCharsetsArePreferred()
    {
        final String longJzonContentType = JZON_CONTENT_TYPE + "; charset=UTF-16";

        RiakObject riakObject = new RiakObject();
        riakObject.setContentType(JZON_CONTENT_TYPE + "; charset=UTF-16");
        riakObject.setCharset(UTF_48_CHARSET);
        riakObject.setValue(BinaryValue.create("foo"));

        assertTrue(riakObject.hasCharset());

        final RiakKvPB.RpbContent pbObject = RiakObjectConverter.convert(riakObject);
        assertTrue(pbObject.hasCharset());
        assertTrue(pbObject.hasContentType());

        assertEquals(UTF_48_CHARSET, pbObject.getCharset().toStringUtf8());
        assertEquals(longJzonContentType, pbObject.getContentType().toStringUtf8());
    }

    @Test
    public void charsetsAreBackwardsCompatible()
    {
        final String longJzonContentType = JZON_CONTENT_TYPE + "; charset=UTF-16";

        RiakObject riakObject = new RiakObject();
        riakObject.setContentType(JZON_CONTENT_TYPE + "; charset=UTF-16");
        riakObject.setValue(BinaryValue.create("foo"));

        assertTrue(riakObject.hasCharset());

        final RiakKvPB.RpbContent pbObject = RiakObjectConverter.convert(riakObject);
        assertTrue(pbObject.hasCharset());
        assertTrue(pbObject.hasContentType());

        assertEquals("UTF-16", pbObject.getCharset().toStringUtf8());
        assertEquals(longJzonContentType, pbObject.getContentType().toStringUtf8());
        // We won't remove it from the Content-Type if declared there, but it may be duplicated like with the old style.
    }
}
