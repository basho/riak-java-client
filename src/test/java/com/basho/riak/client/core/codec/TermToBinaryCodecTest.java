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
    private static final String QUERY = "SELECT * FROM FRAZZLE";

    @Test
    public void encodesGetRequestCorrectly() {
        // {tsgetreq, <<"test_table">>, [<<"series">>, <<"family">>, 12345678], 5000}
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

    @Test
    public void encodesQueryRequestCorrectly() {
        // {tsqueryreq, {tsinterpolation, <<"SELECT * FROM FRAZZLE">>, []}, false, []}
        final byte[] exp = {(byte)131, 104, 4, 100, 0, 10, 116, 115, 113, 117, 101, 114, 121, 114, 101,
            113, 104, 3, 100, 0, 15, 116, 115, 105, 110, 116, 101, 114, 112, 111,
            108, 97, 116, 105, 111, 110, 109, 0, 0, 0, 21, 83, 69, 76, 69, 67, 84,
            32, 42, 32, 70, 82, 79, 77, 32, 70, 82, 65, 90, 90, 76, 69, 106, 100, 0,
            5, 102, 97, 108, 115, 101, 106};

        try {
            OtpOutputStream os = TermToBinaryCodec.encodeTsQueryRequest(QUERY);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }
}