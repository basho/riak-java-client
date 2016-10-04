package com.basho.riak.client.core.query.UserMetadata;

import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.nio.charset.Charset;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RiakUserMetadataTest
{
    final Charset utf16 = Charset.forName("UTF-16");
    final String oddKey = "\uD834\uDD1E\uD835\uDD65";
    final String oddValue = "\uD835\uDFF6\uD840\uDC8A";
    final BinaryValue oddKeyBV = BinaryValue.create(oddKey, utf16);
    final BinaryValue oddValueBV = BinaryValue.create(oddValue, utf16);

    @Test
    public void getSetMetadata_string_altCharset()
    {
        RiakUserMetadata md = new RiakUserMetadata();
        md.put(oddKey, oddValue, utf16);
        final String gotValue = md.get(oddKey, utf16);

        assertEquals(oddValue, gotValue);
        assertNotEquals(gotValue, md.get(oddKey));
    }

    @Test
    public void getSetMetadata_binaryValue_altCharset()
    {
        RiakUserMetadata md = new RiakUserMetadata();
        md.put(oddKeyBV, oddValueBV);
        final BinaryValue gotValue = md.get(oddKeyBV);

        assertEquals(oddValueBV, gotValue);
        assertNotEquals(gotValue, md.get(oddKey));
    }

    @Test
    public void getSetMetadata_mixedInterfaces_altCharset()
    {
        RiakUserMetadata md = new RiakUserMetadata();
        md.put(oddKeyBV, oddValueBV);

        final String gotValue = md.get(oddKey, utf16);

        assertEquals(oddValue, gotValue);
    }
}
