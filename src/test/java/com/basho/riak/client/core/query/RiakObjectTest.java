package com.basho.riak.client.core.query;

import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.query.links.RiakLink;
import com.basho.riak.client.core.util.BinaryValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RiakObjectTest
{
    static VClock vClock = new BasicVClock(new byte[] {'1'});

    @Test
    public void testEqualsWithRiakObject()
    {
        final RiakObject riakObject1 = CreateFilledObject();
        final RiakObject riakObject2 = CreateFilledObject();

        assertEquals(riakObject1, riakObject2);
    }

    public static RiakObject CreateFilledObject()
    {
        final RiakObject result = new RiakObject();
        result.setValue(BinaryValue.create(new byte[] {'O', '_', 'o'}));
        result.getIndexes().getIndex(StringBinIndex.named("foo")).add("bar");
        result.getLinks().addLink(new RiakLink("bucket", "linkkey", "linktag"));
        result.getUserMeta().put("foo", "bar");
        result.setVTag("vtag");
        result.setVClock(vClock);
        return result;
    }
}
