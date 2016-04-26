package com.basho.riak.client.core.operations;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Operations Class Unit Tests
 *
 * @author Alex Moore <amoore at basho dot com>
 * @since 2.0.3
 */
public class OperationsTest
{
    @Test
    public void testThatMessageCodesAreWrittenCorrectlyInErrorMessages()
    {
        // MSG_StartTls = 255
        RiakMessage msg = null;

        try
        {
            msg = new RiakMessage(RiakMessageCodes.MSG_StartTls, new byte[0]);
        }
        catch (Exception ex)
        {
            assertTrue("unexpected exception", false);
        }

        try
        {
            Operations.checkPBMessageType(msg, RiakMessageCodes.MSG_GetReq);
        }
        catch (IllegalStateException ex)
        {
            assertTrue(ex.getMessage().indexOf("255") > 0);
        }
    }

    @Test
    public void testThatSingedToUnsignedConversionIsCorrect()
    {
        assertEquals(Operations.getUnsignedByteValue((byte) 0x00), 0);
        assertEquals(Operations.getUnsignedByteValue((byte) 0xFF), 255);
        assertEquals(Operations.getUnsignedIntValue(0x00000000), 0l);
        assertEquals(Operations.getUnsignedIntValue(0xffffffff), 4294967295l);
    }
}
