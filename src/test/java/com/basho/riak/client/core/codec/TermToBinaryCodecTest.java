package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
    public void encodesPutRequestCorrectly_1() {
        // {tsputreq, <<"test_table">>, [], [{<<"varchar">>, 12345678, 12.34, true, 12345}, {<<"string">>, 8765432, 43.21, false, 543321}]}
        final byte[] exp = {(byte)131, 104, 4, 100, 0, 8, 116, 115, 112, 117, 116, 114, 101, 113, 109, 0,
            0, 0, 10, 116, 101, 115, 116, 95, 116, 97, 98, 108, 101, 106, 108, 0, 0,
            0, 2, 104, 5, 109, 0, 0, 0, 7, 118, 97, 114, 99, 104, 97, 114, 98, 0, (byte)188,
            97, 78,
            70, 64, 65, 38, 102, 102, 102, 102, 102,
            100, 0, 4, 116,
            114, 117, 101, 98, 0, 0, 48, 57, 104, 5, 109, 0, 0, 0, 6, 115, 116, 114,
            105, 110, 103, 98, 0, (byte)133, (byte)191, (byte)248,
            70, 64, 65, 38, 102, 102, 102, 102, 102,
            100, 0, 5, 102, 97, 108, 115, 101, 98, 0, 8, 74, 89, 106};

        Cell c1 = new Cell("varchar");
        Cell c2 = new Cell(12345678L);
        Cell c3 = new Cell(34.3);
        Cell c4 = new Cell(true);
        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeInMillis(12345);
        Cell c5 = new Cell(cal1);
        Row r1 = new Row(c1, c2, c3, c4, c5);

        Cell c6 = new Cell("string");
        Cell c7 = new Cell(8765432L);
        Cell c8 = new Cell(34.3);
        Cell c9 = new Cell(false);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeInMillis(543321);
        Cell c10 = new Cell(cal2);
        Row r2 = new Row(c6, c7, c8, c9, c10);
        Row[] rows = { r1, r2 };

        try {
            OtpOutputStream os = TermToBinaryCodec.encodeTsPutRequest(TABLE_NAME, Arrays.asList(rows));
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesPutRequestCorrectly_2() {
        // A = riakc_ts_put_operator:serialize(<<"test_table">>,[{<<"series">>, <<"family">>, 12345678, 1, true, 34.3, []}], true).
        // A = {tsputreq,<<"test_table">>,[],[{<<"series">>,<<"family">>,12345678,1,true,34.3,[]}]}
        final byte[] exp = {(byte)131,104,4, // outer tuple arity 4
                            100,0,8,116,115,112,117,116,114,101,113, // tsputreq atom
                            109,0,0,0,10,116,101,115,116,95,116,97,98,108,101, // table name binary
                            106, // empty list
                            108,0,0,0,1, // list start arity 1
                                104,7, // row tuple arity 7
                                    109,0,0,0,6,115,101,114,105,101,115, // series binary
                                    109,0,0,0,6,102,97,109,105,108,121, // family binary
                                    98,0,(byte)188,97,78, // integer
                                    97,1, // small integer
                                    100,0,4,116,114,117,101, // true atom
                                    // NB: this is what Erlang generates, an old-style float
                                    // 99,51,46,52,50,57,57,57,57,57,57,57,57,57,57,57,57,57,55,49,53,55,56,101,43,48,49,0,0,0,0,0, // float_ext len 31
                                    // NB: this is what JInterface generates, a new-style float
                                    70, 64, 65, 38, 102, 102, 102, 102, 102,
                                    106, // null cell empty list
                            106}; // list arity 1 end

        final ArrayList<Row> rows = new ArrayList<>(1);
        rows.add(new Row(new Cell("series"), new Cell("family"), Cell.newTimestamp(12345678),
                         new Cell(1L), new Cell(true), new Cell(34.3), null));

        try {
            OtpOutputStream os = TermToBinaryCodec.encodeTsPutRequest(TABLE_NAME, rows);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }
    }

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
