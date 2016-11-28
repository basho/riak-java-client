package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.ColumnDescription;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpOutputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 *
 * @author Alex Moore <amoore at basho dot com>
 * @author Luke Bakken <lbakken at basho dot com>
 */
public class TermToBinaryCodecTest
{
    private static final String TABLE_NAME = "test_table";
    private static final String QUERY = "SELECT * FROM FRAZZLE";
    private static final byte[] CONTEXT = new byte[] {(byte)131,104,2,98,40,26,4,(byte)204,109,0,0,0,12,
                                                      (byte)131,104,1,100,0,6,102,111,111,98,97,114};

    @Test
    public void encodesPutRequestCorrectly_1()
    {
        // {tsputreq, <<"test_table">>, [], [{<<"varchar">>, 12345678, 34.3, true, 12345, <<0,1,2,3,4,5,6,7>>},
        //                                   {<<"string">>, 8765432, 34.3, false, 543321, <<8,9,10,11,12,13,14,15>>}]}
        final byte[] exp = {(byte)131, 104, 4, // outer tuple arity 4
                            100, 0, 8, 116, 115, 112, 117, 116, 114, 101, 113, // tsputreq atom
                            109, 0, 0, 0, 10, 116, 101, 115, 116, 95, 116, 97, 98, 108, 101, //  table name
                            106, // empty list
                            108, 0, 0, 0, 2, // list start arity 2
                                104, 6, // row tuple arity 6
                                    109, 0, 0, 0, 7, 118, 97, 114, 99, 104, 97, 114, // varchar cell
                                    98, 0, (byte)188, 97, 78, // integer
                                    70, 64, 65, 38, 102, 102, 102, 102, 102, // 34.3 float
                                    100, 0, 4, 116, 114, 117, 101, // true atom
                                    98, 0, 0, 48, 57, // integer
                                    109,0,0,0,8,0,1,2,3,4,5,6,7, // blob cell
                                104, 6, // row tuple arity 6
                                    109, 0, 0, 0, 6, 115, 116, 114, 105, 110, 103, // varchar cell "string"
                                    98, 0, (byte)133, (byte)191, (byte)248, // integer
                                    70, 64, 65, 38, 102, 102, 102, 102, 102, // 34.3 float
                                    100, 0, 5, 102, 97, 108, 115, 101, // false atom
                                    98, 0, 8, 74, 89,  // integer
                                    109,0,0,0,8,8,9,10,11,12,13,14,15, // blob cell
                            106}; // end list arity 2

        Cell r1c1 = new Cell("varchar");
        Cell r1c2 = new Cell(12345678L);
        Cell r1c3 = new Cell(34.3);
        Cell r1c4 = new Cell(true);
        Calendar cal1 = Calendar.getInstance();
        cal1.setTimeInMillis(12345);
        Cell r1c5 = new Cell(cal1);
        Cell r1c6 = new Cell(new byte[] {0,1,2,3,4,5,6,7});
        Row r1 = new Row(r1c1, r1c2, r1c3, r1c4, r1c5, r1c6);

        Cell r2c1 = new Cell("string");
        Cell r2c2 = new Cell(8765432L);
        Cell r2c3 = new Cell(34.3);
        Cell r2c4 = new Cell(false);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTimeInMillis(543321);
        Cell r2c5 = new Cell(cal2);
        Cell r2c6 = new Cell(new byte[] {8,9,10,11,12,13,14,15});
        Row r2 = new Row(r2c1, r2c2, r2c3, r2c4, r2c5, r2c6);
        Row[] rows = {r1, r2};

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsPutRequest(TABLE_NAME, Arrays.asList(rows));
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesPutRequestCorrectly_2()
    {
        // A = riakc_ts_put_operator:serialize(<<"test_table">>,
        //                                     [{<<"series">>, <<"family">>, 12345678, 1, true, 34.3, []}],
        //                                     true).
        // A = {tsputreq,<<"test_table">>,[],[{<<"series">>,<<"family">>,12345678,1,true,34.3,[]}]}
        final byte[] exp = {(byte)131,104,4, // outer tuple arity 4
                            100,0,8,116,115,112,117,116,114,101,113, // tsputreq atom
                            109,0,0,0,10,116,101,115,116,95,116,97,98,108,101, // table name binary
                            106, // empty list
                            108,0,0,0,1, // list start arity 1
                                104,8, // row tuple arity 7
                                    109,0,0,0,6,115,101,114,105,101,115, // series binary
                                    109,0,0,0,6,102,97,109,105,108,121, // family binary
                                    98,0,(byte)188,97,78, // integer
                                    97,1, // small integer
                                    100,0,4,116,114,117,101, // true atom
                                    // NB: this is what Erlang generates, an old-style float
                                    // 99,51,46,52,50,57,57,57,57,57,57,57,57,57,
                                    //   57,57,57,57,55,49,53,55,56,101,43,48,49,0,0,0,0,0, // float_ext len 31
                                    // NB: this is what JInterface generates, a new-style float
                                    70,64,65,38,102,102,102,102,102,
                                    106, // null cell empty list
                                    109,0,0,0,8,0,1,2,3,4,5,6,7, // blob cell
                            106}; // list arity 1 end

        final ArrayList<Row> rows = new ArrayList<>(1);
        rows.add(new Row(new Cell("series"), new Cell("family"), Cell.newTimestamp(12345678),
                         new Cell(1L), new Cell(true), new Cell(34.3), null, new Cell(new byte[] {0,1,2,3,4,5,6,7})));

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsPutRequest(TABLE_NAME, rows);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesPutWithColumnsCorrectly()
    {
        /*
            {tsputreq,<<"test_table">>,
                      [<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>,<<"f">>,<<"g">>, ],
                      [{<<"series">>,<<"family">>,12345678,1,true,34.3,[], <<0,1,2,3,4,5,6,7>>}]}
         */
        final byte[] exp = {
                (byte)131,104,4,
                    100,0,8,116,115,112,117,116,114,101,113,
                    109,0,0,0,10,116,101,115,116,95,116,97,98,108,101,
                    108,0,0,0,7,
                        109,0,0,0,1,97,
                        109,0,0,0,1,98,
                        109,0,0,0,1,99,
                        109,0,0,0,1,100,
                        109,0,0,0,1,101,
                        109,0,0,0,1,102,
                        109,0,0,0,1,103,
                        106,
                    108,0,0,0,1,
                        104,8,
                            109,0,0,0,6,115,101,114,105,101,115,
                            109,0,0,0,6,102,97,109,105,108,121,
                            98,0,(byte)188,97,78,
                            97,1,
                            100,0,4,116,114,117,101,
                            // NB: this is what Erlang generates, an old-style float
                            // 99,51,46,52,50,57,57,57,57,57,57,57,57,57,57,57,57,
                            //     57,55,49,53,55,56,101,43,48,49,0,0,0,0,0, // float_ext len 31
                            // NB: this is what JInterface generates, a new-style float
                            70,64,65,38,102,102,102,102,102,
                            106,
                            109,0,0,0,8,0,1,2,3,4,5,6,7,
                        106};

        final List<String> columns = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
        final ArrayList<Row> rows = new ArrayList<>(1);
        rows.add(new Row(new Cell("series"), new Cell("family"), Cell.newTimestamp(12345678),
                         new Cell(1L), new Cell(true), new Cell(34.3), null, new Cell(new byte[] {0,1,2,3,4,5,6,7})));

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsPutRequest(TABLE_NAME, columns, rows);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesGetRequestCorrectly()
    {
        // {tsgetreq, <<"test_table">>, [<<"series">>, <<"family">>, 12345678], 5000}
        final byte[] exp = {(byte)131, 104, 4, 100, 0, 8, 116, 115, 103, 101, 116, 114, 101, 113, 109, 0,
            0, 0, 10, 116, 101, 115, 116, 95, 116, 97, 98, 108, 101, 108, 0, 0, 0, 3,
            109, 0, 0, 0, 6, 115, 101, 114, 105, 101, 115, 109, 0, 0, 0, 6, 102, 97,
            109, 105, 108, 121, 98, 0, (byte)188, 97, 78, 106, 98, 0, 0, 19, (byte)136};

        Cell k1 = new Cell("series");
        Cell k2 = new Cell("family");
        Cell k3 = new Cell(12345678);
        Cell[] key = {k1, k2, k3};

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsGetRequest(TABLE_NAME, Arrays.asList(key), 5000);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesQueryRequestCorrectly()
    {
        // {tsqueryreq,{tsinterpolation,<<"SELECT * FROM FRAZZLE">>,[]},false,undefined}
        final byte[] exp = {(byte)131,104,5,100,0,10,116,115,113,117,101,114,121,114,101,
                            113,104,3,100,0,15,116,115,105,110,116,101,114,112,111,
                            108,97,116,105,111,110,109,0,0,0,21,83,69,76,69,67,84,
                            32,42,32,70,82,79,77,32,70,82,65,90,90,76,69,106,100,0,
                            5,102,97,108,115,101,100,0,9,117,110,100,101,102,105,
                            110,101,100};

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsQueryRequest(QUERY, null);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesQueryRequestWithCoverageContextCorrectly()
    {
        // {tsqueryreq, {tsinterpolation, <<"SELECT * FROM FRAZZLE">>, []},
        //              false,
        //              <<131,104,2,98,40,26,4,204,109,0,0,0,12,131,104,1,100,0,6,102,111,111,98,97,114>>}
        final byte[] exp = {(byte)131,104,5,100,0,10,116,115,113,117,101,114,121,114,101,
                            113,104,3,100,0,15,116,115,105,110,116,101,114,112,111,
                            108,97,116,105,111,110,109,0,0,0,21,83,69,76,69,67,84,
                            32,42,32,70,82,79,77,32,70,82,65,90,90,76,69,106,100,0,
                            5,102,97,108,115,101,109,0,0,0,25,(byte)131,104,2,98,40,26,4,
                            (byte)204,109,0,0,0,12,(byte)131,104,1,100,0,6,102,111,111,98,97,114};

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsQueryRequest(QUERY, CONTEXT);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void encodesQueryRequestWithCoverageContextAndBufferReuseCorrectly()
    {
        // {tsqueryreq, {tsinterpolation, <<"SELECT * FROM FRAZZLE">>, []},
        //              false,
        //              <<131,104,2,98,40,26,4,204,109,0,0,0,12,131,104,1,100,0,6,102,111,111,98,97,114>>,
        //              true}
        final byte[] exp = {(byte)131,104,5,100,0,10,116,115,113,117,101,114,121,114,101,
                            113,104,3,100,0,15,116,115,105,110,116,101,114,112,111,
                            108,97,116,105,111,110,109,0,0,0,21,83,69,76,69,67,84,
                            32,42,32,70,82,79,77,32,70,82,65,90,90,76,69,106,100,0,
                            5,102,97,108,115,101,109,0,0,0,25,(byte)131,104,2,98,40,26,4,
                            (byte)204,109,0,0,0,12,(byte)131,104,1,100,0,6,102,111,111,98,97,114};

        try
        {
            OtpOutputStream os = TermToBinaryCodec.encodeTsQueryRequest(QUERY, CONTEXT);
            os.flush();
            byte[] msg = os.toByteArray();
            Assert.assertArrayEquals(exp, msg);
        }
        catch (IOException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void decodesQueryResultCorrectly() throws OtpErlangDecodeException
    {
        /* MSG = {tsqueryresp, DATA}
           DATA = {COLUMN_NAMES, COLUMN_TYPES, ROWS}
           COLUMN_NAMES = [binary, ...]
           COLUMN_TYPES = [atom, ...]
           ROWS = [ ROW, ...]
           ROW = { binary :: numeric :: atom :: [], ... }

           { tsqueryresp,
             { [<<"geohash">>,<<"user">>,<<"time">>,<<"weather">>,<<"temperature">>,<<"uv_index">>,<<"observed">>,<<"sensor_data">>],
               [varchar,varchar,timestamp,varchar,double,sint64,boolean,blob],
               [
                 {<<"hash1">>,<<"user2">>,1443806600000,<<"cloudy">>,[],[],true,<<0,1,2,3,4,5,6,7>>}
               ]
             }
           }
         */

        final byte[] input =
                {(byte)131, 104, 2, // outer tuple arity 2
                    100, 0, 11, 116, 115, 113, 117, 101, 114, 121, 114, 101, 115, 112, 104, 3, // tsqueryresp atom
                    108, 0, 0, 0, 8, // list start arity 8
                        109, 0, 0, 0, 7, 103, 101, 111, 104, 97, 115, 104, // "geohash"
                        109, 0, 0, 0, 4, 117, 115, 101, 114, // "user
                        109, 0, 0, 0, 4, 116, 105, 109, 101, // "time"
                        109, 0, 0, 0, 7, 119, 101, 97, 116, 104, 101, 114, // "weather"
                        109, 0, 0, 0, 11, 116, 101, 109, 112, 101, 114, 97, 116, 117, 114, 101, // "temperature"
                        109, 0, 0, 0, 8, 117, 118, 95, 105, 110, 100, 101, 120, // "uv_index"
                        109, 0, 0, 0, 8, 111, 98, 115, 101, 114, 118, 101, 100, // "observed"
                        109, 0, 0, 0, 11, 115, 101, 110, 115, 111, 114, 95, 100, 97, 116, 97, // "sensor_data"
                    106, // list stop arity 8
                    108, 0, 0, 0, 8, // list start arity 8
                        100, 0, 7, 118, 97, 114, 99, 104, 97, 114, // varchar atom
                        100, 0, 7, 118, 97, 114, 99, 104, 97, 114, // varchar atom
                        100, 0, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, // timestamp atom
                        100, 0, 7, 118, 97, 114, 99, 104, 97, 114, // varchar atom
                        100, 0, 6, 100, 111, 117, 98, 108, 101, // double atom
                        100, 0, 6, 115, 105, 110, 116, 54, 52, // sint64 atom
                        100, 0, 7, 98, 111, 111, 108, 101, 97, 110, // boolean atom
                        100, 0, 4, 98, 108, 111, 98, // blob atom
                    106, // list stop arity 8
                    108, 0, 0, 0, 1, // list start arity 1
                        104, 8, // tuple arity 8
                            109, 0, 0, 0, 5, 104, 97, 115, 104, 49, // "hash1"
                            109, 0, 0, 0, 5, 117, 115, 101, 114, 50, // "user 2
                            110, 6, 0, 64, 91, -108, 41, 80, 1, // 1443806600000
                            109, 0, 0, 0, 6, 99, 108, 111, 117, 100, 121, // "cloudy"
                            106, // empty list / null cell
                            106, // empty list / null cell
                            100, 0, 4, 116, 114, 117, 101, // true atom
                            109, 0, 0, 0, 8, 0, 1, 2, 3, 4, 5, 6, 7, // <<0,1,2,3,4,5,6,7>>
                 106};

        final ColumnDescription[] expectedColumnDescriptions = new ColumnDescription[8];
        expectedColumnDescriptions[0] = new ColumnDescription("geohash", ColumnDescription.ColumnType.VARCHAR);
        expectedColumnDescriptions[1] = new ColumnDescription("user", ColumnDescription.ColumnType.VARCHAR);
        expectedColumnDescriptions[2] = new ColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP);
        expectedColumnDescriptions[3] = new ColumnDescription("weather", ColumnDescription.ColumnType.VARCHAR);
        expectedColumnDescriptions[4] = new ColumnDescription("temperature", ColumnDescription.ColumnType.DOUBLE);
        expectedColumnDescriptions[5] = new ColumnDescription("uv_index", ColumnDescription.ColumnType.SINT64);
        expectedColumnDescriptions[6] = new ColumnDescription("observed", ColumnDescription.ColumnType.BOOLEAN);
        expectedColumnDescriptions[7] = new ColumnDescription("sensor_data", ColumnDescription.ColumnType.BLOB);

        final Row row = new Row(new Cell("hash1"), new Cell("user2"), Cell.newTimestamp(1443806600000L),
                                new Cell("cloudy"), null, null, new Cell(true), new Cell(new byte[] {0,1,2,3,4,5,6,7}));
        final Row[] expectedRows = new Row[1];
        expectedRows[0] = (row);

        try
        {
            final QueryResult actual = TermToBinaryCodec.decodeTsResultResponse(input);

            final List<ColumnDescription> actualColumnDescriptions = actual.getColumnDescriptionsCopy();
            final List<Row> actualRows = actual.getRowsCopy();

            Assert.assertArrayEquals(expectedColumnDescriptions,
                                     actualColumnDescriptions.toArray(
                                             new ColumnDescription[actualColumnDescriptions.size()]));

            Assert.assertArrayEquals(expectedRows, actualRows.toArray(new Row[actualRows.size()]));
        }
        catch (InvalidTermToBinaryException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }
}
