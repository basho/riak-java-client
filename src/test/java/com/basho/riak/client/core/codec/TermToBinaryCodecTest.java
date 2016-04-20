package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Luke Bakken <lbakken at basho dot com>
 */
public class TermToBinaryCodecTest
{
    private static final String TABLE_NAME = "test_table";

    @Test
    public void encodesGetRequestCorrectly() {
        final byte[] exp = {(byte)131, 104, 4, 100, 0, 8, 116, 115, 103, 101, 116, 114, 101, 113, 109, 0,
            0, 0, 10, 116, 101, 115, 116, 95, 116, 97, 98, 108, 101, 108, 0, 0, 0, 3,
            109, 0, 0, 0, 6, 115, 101, 114, 105, 101, 115, 109, 0, 0, 0, 6, 102, 97,
            109, 105, 108, 121, 98, 0, (byte)188, 97, 78, 106, 98, 0, 0, 19, (byte)136};

        Cell k1 = new Cell("series");
        Cell k2 = new Cell("family");
        Cell k3 = new Cell(12345678);
        Cell[] key = {k1, k2, k3};

        try {
            OtpOutputStream os = TermToBinaryCodec.encodeTsGetRequest(TABLE_NAME, Arrays.asList(key), 5000);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }
}