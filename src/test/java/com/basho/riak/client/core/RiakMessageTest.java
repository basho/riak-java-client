package com.basho.riak.client.core;

import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Luke Bakken <lbakken at basho dot com>
 */
public class RiakMessageTest
{
    @Test
    public void parsesPbufErrorCorrectly()
    {
        RiakPB.RpbErrorResp.Builder b = RiakPB.RpbErrorResp.newBuilder();
        b.setErrcode(1234);
        b.setErrmsg(ByteString.copyFromUtf8("this is an error"));
        RiakPB.RpbErrorResp rpbErrorResp = b.build();

        RiakMessage msg = new RiakMessage(RiakMessageCodes.MSG_ErrorResp, rpbErrorResp.toByteArray());
        RiakResponseException err = msg.getRiakError();
        Assert.assertEquals("this is an error", err.getMessage());
        Assert.assertEquals(1234, err.getCode());
    }

    @Test
    public void parsesTtbErrorCorrectly()
    {
        final byte[] TTB_ERROR = {(byte)131, 104, 3, 100, 0, 12, 114, 112, 98, 101, 114,
            114, 111, 114, 114, 101, 115, 112, 109, 0, 0, 0, 16, 116, 104, 105, 115, 32, 105, 115,
            32, 97, 110, 32, 101, 114, 114, 111, 114, 98, 0, 0, 4, (byte)210};

        RiakMessage msg = new RiakMessage(RiakMessageCodes.MSG_TsTtbMsg, TTB_ERROR);
        RiakResponseException err = msg.getRiakError();
        Assert.assertEquals("this is an error", err.getMessage());
        Assert.assertEquals(1234, err.getCode());
    }
}