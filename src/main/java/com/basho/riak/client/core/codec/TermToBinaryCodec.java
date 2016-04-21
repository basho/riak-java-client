package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpExternal;
import com.ericsson.otp.erlang.OtpInputStream;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collection;

public class TermToBinaryCodec
{
    private static final OtpErlangAtom undefined = new OtpErlangAtom("undefined");
    private static final OtpErlangAtom tsinterpolation = new OtpErlangAtom("tsinterpolation");

    private static final OtpErlangAtom _false = new OtpErlangAtom("false");
    private static final OtpErlangList EMPTY_ERLANG_LIST = new OtpErlangList();

    private static class Messages
    {
        public static final OtpErlangAtom tsGetReq = new OtpErlangAtom("tsgetreq");
        public static final OtpErlangAtom tsGetResp = new OtpErlangAtom("tsgetresp");

        public static final OtpErlangAtom tsQueryReq = new OtpErlangAtom("tsqueryreq");
        public static final OtpErlangAtom tsQueryResp = new OtpErlangAtom("tsqueryresp");
        public static final OtpErlangAtom tsInterpolation = new OtpErlangAtom("tsinterpolation");

        public static final OtpErlangAtom tsPutReq = new OtpErlangAtom("tsputreq");
        public static final OtpErlangAtom tsPutResp = new OtpErlangAtom("tsputresp");

        public static final OtpErlangAtom tsDelReq = new OtpErlangAtom("tsdelreq");
        public static final OtpErlangAtom tsDelResp = new OtpErlangAtom("tsdelresp");

        public static final OtpErlangAtom rpbErrorResp = new OtpErlangAtom("rpberrorresp");
    }

    public static OtpOutputStream encodeTsGetRequest(String tableName, Collection<Cell> keyValues, int timeout)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // NB: TsGetReq is a 4-tuple: tsgetreq, tableName, [key values], timeout
        os.write_tuple_head(4);
        os.write_any(Messages.tsGetReq);
        os.write_binary(tableName.getBytes(StandardCharsets.UTF_8));

        os.write_list_head(keyValues.size());
        for (Cell k : keyValues) {
            os.write_any(k.getErlangObject());
        }
        os.write_nil(); // NB: finishes the list

        os.write_long(timeout);

        return os;
    }

    public static QueryResult decodeTsGetResponse(byte[] response) throws OtpErlangDecodeException
    {
        return decodeTsResponse(response);
    }

    public static OtpOutputStream encodeTsQueryRequest(String queryText)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // TsQueryReq is a 4-tuple: {'tsqueryreq', TsInt, boolIsStreaming, bytesCoverContext}
        os.write_tuple_head(4);
        os.write_any(Messages.tsQueryReq);

        // TsInterpolation is a 3-tuple
        // {'tsinterpolation', query, []} empty list is interpolations
        os.write_tuple_head(3);
        os.write_any(Messages.tsInterpolation);
        os.write_binary(queryText.getBytes(StandardCharsets.UTF_8));
        // interpolations is an empty list
        os.write_nil();

        // streaming is false for now
        os.write_boolean(false);

        // cover_context is an empty list
        os.write_nil();

        return os;
    }

    public static QueryResult decodeTsQueryResponse(byte[] response) throws OtpErlangDecodeException
    {
        return decodeTsResponse(response);
    }

    public static OtpOutputStream encodeTsPutRequest(String tableName, Collection<Row> rows)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // TsPutReq is a 4-tuple: {'tsputreq', tableName, [], [rows]}
        // columns is empte
        os.write_tuple_head(4);
        os.write_any(Messages.tsPutReq);
        os.write_binary(tableName.getBytes(StandardCharsets.UTF_8));
        // columns is an empty list
        os.write_nil();

        // write a list of rows
        // each row is a tuple of cells
        os.write_list_head(rows.size());
        for (Row r : rows) {
            os.write_tuple_head(r.getCellsCount());
            for (Cell c : r) {
                if (c == null) {
                    // NB: Null cells are represented as empty lists
                    os.write_nil();
                } else {
                    os.write_any(c.getErlangObject());
                }
            }
        }
        os.write_nil();

        return os;
    }

    public static Void decodeTsPutResponse(byte[] response)
    {
        // Do we return anything in TTB?
        return null;
    }

    private static QueryResult decodeTsResponse(byte[] response) throws OtpErlangDecodeException
    {
        QueryResult result = null;

        OtpInputStream is = new OtpInputStream(response);
        final int msgArity = is.read_tuple_head();
        // Response is:
        // {'rpberrorresp', ErrMsg, ErrCode}
        // {'tsgetresp', {ColNames, ColTypes, Rows}}
        // {'tsqueryresp', {ColNames, ColTypes, Rows}}
        final String respAtom = is.read_atom();
        switch (respAtom) {
            case "rpberrorresp":
                // TODO process error
                assert(msgArity == 3);
                break;
            case "tsgetresp":
            case "tsqueryresp":
                assert(msgArity == 2);

                final int dataArity = is.read_tuple_head();
                assert(dataArity == 3);

                final int colNameCount = is.read_list_head();
                String[] columnNames = new String[colNameCount];
                for (int i = 0; i < colNameCount; i++) {
                    String colName = is.read_string();
                    columnNames[i] = colName;
                }

                final int colTypeCount = is.read_list_head();
                assert(colNameCount == colTypeCount);
                String[] columnTypes = new String[colTypeCount];
                for (int i = 0; i < colTypeCount; i++) {
                    String colType = is.read_string();
                    columnTypes[i] = colType;
                }

                final int rowCount = is.read_list_head();
                Row[] rows = new Row[rowCount];
                for (int i = 0; i < rowCount; i++) {
                    final int rowDataCount = is.read_tuple_head();
                    assert(colNameCount == rowDataCount);

                    Cell[] cells = new Cell[rowDataCount];
                    for (int j = 0; i < rowDataCount; j++) {
                        OtpErlangObject cell = is.read_any();
                        if (cell instanceof OtpErlangBinary) {
                            OtpErlangBinary v = (OtpErlangBinary)cell;
                            // TODO GH-611 this may not be correct encoding
                            String s = new String(v.binaryValue(), StandardCharsets.UTF_8);
                            cells[j] = new Cell(s);
                        }
                        else if (cell instanceof OtpErlangBoolean) {
                            OtpErlangBoolean v = (OtpErlangBoolean)cell;
                            cells[j] = new Cell(v.booleanValue());
                        }
                        else if (cell instanceof OtpErlangLong) {
                            OtpErlangLong v = (OtpErlangLong)cell;
                            if ("timestamp".equals(columnTypes[j])) {
                                Calendar cal = Calendar.getInstance();
                                cal.setTimeInMillis(v.longValue());
                                cells[j] = new Cell(cal);
                            } else {
                                cells[j] = new Cell(v.longValue());
                            }
                        }
                        else if (cell instanceof OtpErlangDouble) {
                            OtpErlangDouble v = (OtpErlangDouble)cell;
                            cells[j] = new Cell(v.doubleValue());
                        }
                        else {
                            // TODO GH-611 throw exception?
                        }
                    }

                    rows[i] = new Row(cells);
                }

                result = new QueryResult(rows);

                break;
            default:
                // TODO GH-611 throw exception?
        }

        return result;
    }
}
