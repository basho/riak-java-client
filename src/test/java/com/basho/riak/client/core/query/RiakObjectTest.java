package com.basho.riak.client.core.query;

import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.core.query.indexes.*;
import com.basho.riak.client.core.query.links.RiakLink;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import java.io.*;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

public class RiakObjectTest
{
    static VClock vClock = new BasicVClock(new byte[] {'1'});

    @Test
    public void testEqualsWithRiakObject()
    {
        final RiakObject riakObject1 = createFilledObject();
        final RiakObject riakObject2 = createFilledObject();

        assertEquals(riakObject1, riakObject2);
    }

    @Test
    public void checkSerialization() throws IOException, ClassNotFoundException {
        final RiakObject ro = createFilledObject();

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bos);

        out.writeObject(ro);
        out.close();

        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        final ObjectInputStream in = new ObjectInputStream(bis);

        final RiakObject ro2 = (RiakObject) in.readObject();
        assertEquals(ro, ro2);
    }

    public static RiakObject createFilledObject()
    {
        final RiakObject result = new RiakObject();
        result.setValue(BinaryValue.create(new byte[] {'O', '_', 'o'}));
        result.getIndexes().getIndex(StringBinIndex.named("foo")).add("bar");
        result.getIndexes().getIndex(LongIntIndex.named("foo-long")).add(2l);
        result.getIndexes().getIndex(BigIntIndex.named("foo-bint")).add(BigInteger.ONE);
        result.getIndexes().getIndex(RawIndex.named("foo-raw", IndexType.BUCKET)).add(BinaryValue.create("binary-value"));
        result.getLinks().addLink(new RiakLink("bucket", "linkkey", "linktag"));
        result.getUserMeta().put("foo", "bar");
        result.setVTag("vtag");
        result.setVClock(vClock);
        return result;
    }
}
