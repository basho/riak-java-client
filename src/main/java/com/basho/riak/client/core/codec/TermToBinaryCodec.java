package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.protobuf.RiakTsPB;
import com.ericsson.otp.erlang.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class TermToBinaryCodec
{
    private static final String TS_GET_REQ = "tsgetreq";
    private static final String TS_GET_RESP = "tsgetresp";
    private static final String TS_QUERY_REQ = "tsqueryreq";
    private static final String TS_QUERY_RESP = "tsqueryresp";
    private static final String TS_INTERPOLATION = "tsinterpolation";
    private static final String TS_PUT_REQ = "tsputreq";
    private static final String ERROR_RESP = "rpberrorresp";
    private static final String UNDEFINED = "undefined";
    private static final OtpErlangAtom TS_GET_REQ_ATOM = new OtpErlangAtom(TS_GET_REQ);
    private static final OtpErlangAtom TS_QUERY_REQ_ATOM = new OtpErlangAtom(TS_QUERY_REQ);
    private static final OtpErlangAtom TS_INTERPOLATION_ATOM = new OtpErlangAtom(TS_INTERPOLATION);
    private static final OtpErlangAtom TS_PUT_REQ_ATOM = new OtpErlangAtom(TS_PUT_REQ);
    private static final OtpErlangAtom UNDEFINED_ATOM = new OtpErlangAtom(UNDEFINED);
    private static Logger logger = LoggerFactory.getLogger(TermToBinaryCodec.class);

    public static OtpOutputStream encodeTsGetRequest(String tableName, Collection<Cell> keyValues, int timeout)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // NB: TsGetReq is a 4-tuple: tsgetreq, tableName, [key values], timeout
        os.write_tuple_head(4);
        os.write_any(TS_GET_REQ_ATOM);
        os.write_binary(tableName.getBytes(StandardCharsets.UTF_8));

        os.write_list_head(keyValues.size());
        for (Cell k : keyValues)
        {
            os.write_any(k.getErlangObject());
        }
        os.write_nil(); // NB: finishes the list

        os.write_long(timeout);

        return os;
    }

    public static QueryResult decodeTsResultResponse(byte[] response)
            throws OtpErlangDecodeException, InvalidTermToBinaryException
    {
        return decodeTsResponse(response);
    }

    public static OtpOutputStream encodeTsQueryRequest(String queryText)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // TsQueryReq is a 4-tuple: {'tsqueryreq', TsInt, boolIsStreaming, bytesCoverContext}
        os.write_tuple_head(4);
        os.write_any(TS_QUERY_REQ_ATOM);

        // TsInterpolation is a 3-tuple
        // {'tsinterpolation', query, []} empty list is interpolations
        os.write_tuple_head(3);
        os.write_any(TS_INTERPOLATION_ATOM);
        os.write_binary(queryText.getBytes(StandardCharsets.UTF_8));
        // interpolations is an empty list
        os.write_nil();

        // streaming is false for now
        os.write_boolean(false);

        // cover_context is an undefined atom
        os.write_any(UNDEFINED_ATOM);

        return os;
    }

    public static OtpOutputStream encodeTsPutRequest(String tableName, Collection<Row> rows)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag); // NB: this is the reqired 0x83 (131) value

        // TsPutReq is a 4-tuple: {'tsputreq', tableName, [], [rows]}
        // columns is empte
        os.write_tuple_head(4);
        os.write_any(TS_PUT_REQ_ATOM);
        os.write_binary(tableName.getBytes(StandardCharsets.UTF_8));
        // columns is an empty list
        os.write_nil();

        // write a list of rows
        // each row is a tuple of cells
        os.write_list_head(rows.size());
        for (Row r : rows)
        {
            os.write_tuple_head(r.getCellsCount());
            for (Cell c : r)
            {
                if (c == null)
                {
                    // NB: Null cells are represented as empty lists
                    os.write_nil();
                }
                else
                {
                    os.write_any(c.getErlangObject());
                }
            }
        }
        os.write_nil();

        return os;
    }

    private static QueryResult decodeTsResponse(byte[] response)
            throws OtpErlangDecodeException, InvalidTermToBinaryException
    {
        QueryResult result = null;

        OtpInputStream is = new OtpInputStream(response);
        final int msgArity = is.read_tuple_head();
        // Response is:
        // {'rpberrorresp', ErrMsg, ErrCode}
        // {'tsgetresp', {ColNames, ColTypes, Rows}}
        // {'tsqueryresp', {ColNames, ColTypes, Rows}}
        final String respAtom = is.read_atom();
        switch (respAtom)
        {
            case ERROR_RESP:
                // TODO process error
                assert (msgArity == 3);
                break;
            case TS_GET_RESP:
            case TS_QUERY_RESP:
                assert (msgArity == 2);

                final int dataArity = is.read_tuple_head();
                assert (dataArity == 3);

                final ArrayList<RiakTsPB.TsColumnDescription> columnDescriptions = parseColumnDescriptions(is);

                final ArrayList<RiakTsPB.TsRow> rows = parseRows(is, columnDescriptions);

                result = new QueryResult(columnDescriptions, rows);

                break;
            default:
                final String errorMsg = "Unsupported response message received: " + respAtom;
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
        }

        return result;
    }

    private static ArrayList<RiakTsPB.TsColumnDescription> parseColumnDescriptions(OtpInputStream is)
            throws OtpErlangDecodeException
    {
        final int colNameCount = is.read_list_head();
        final String[] columnNames = new String[colNameCount];
        for (int colNameIdx = 0; colNameIdx < colNameCount; colNameIdx++)
        {
            final String colName = new String(is.read_binary(), StandardCharsets.UTF_8);
            columnNames[colNameIdx] = colName;
        }

        if (colNameCount > 0)
        {
            is.read_nil();
        }


        final int colTypeCount = is.read_list_head();
        assert (colNameCount == colTypeCount);
        final String[] columnTypes = new String[colTypeCount];
        for (int colTypeIdx = 0; colTypeIdx < colTypeCount; colTypeIdx++)
        {
            final String colType = is.read_atom();
            columnTypes[colTypeIdx] = colType;
        }

        if (colTypeCount > 0)
        {
            is.read_nil();
        }

        final ArrayList<RiakTsPB.TsColumnDescription> columnDescriptions = new ArrayList<>(colNameCount);
        for (int colDescIdx = 0; colDescIdx < colNameCount; colDescIdx++)
        {
            final RiakTsPB.TsColumnDescription.Builder descBuilder = RiakTsPB.TsColumnDescription.newBuilder();

            descBuilder.setName(ByteString.copyFromUtf8(columnNames[colDescIdx]));
            descBuilder.setType(RiakTsPB.TsColumnType.valueOf(columnTypes[colDescIdx].toUpperCase(Locale.US)));

            columnDescriptions.add(descBuilder.build());
        }
        return columnDescriptions;
    }

    private static ArrayList<RiakTsPB.TsRow> parseRows(OtpInputStream is,
                                                       List<RiakTsPB.TsColumnDescription> columnDescriptions)
            throws OtpErlangDecodeException, InvalidTermToBinaryException
    {
        final int rowCount = is.read_list_head();
        final ArrayList<RiakTsPB.TsRow> rows = new ArrayList<>(rowCount);

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++)
        {
            rows.add(parseRow(is, columnDescriptions));
        }
        return rows;
    }

    private static RiakTsPB.TsRow parseRow(OtpInputStream is, List<RiakTsPB.TsColumnDescription> columnDescriptions)
            throws OtpErlangDecodeException, InvalidTermToBinaryException
    {
        final int rowDataCount = is.read_tuple_head();
        assert (columnDescriptions.size() == rowDataCount);

        final Cell[] cells = new Cell[rowDataCount];
        for (int j = 0; j < rowDataCount; j++)
        {
            final OtpErlangObject cell = is.read_any();
            cells[j] = parseCell(columnDescriptions, j, cell);
        }

        return new Row(cells).getPbRow();
    }

    private static Cell parseCell(List<RiakTsPB.TsColumnDescription> columnDescriptions, int j, OtpErlangObject cell)
            throws InvalidTermToBinaryException
    {
        if (cell instanceof OtpErlangBinary)
        {
            OtpErlangBinary v = (OtpErlangBinary) cell;
            String s = new String(v.binaryValue(), StandardCharsets.UTF_8);
            return new Cell(s);
        }
        else if (cell instanceof OtpErlangLong)
        {
            OtpErlangLong v = (OtpErlangLong) cell;
            if (columnDescriptions.get(j).getType() == RiakTsPB.TsColumnType.TIMESTAMP)
            {
                return Cell.newTimestamp(v.longValue());
            }
            else
            {
                return new Cell(v.longValue());
            }
        }
        else if (cell instanceof OtpErlangDouble)
        {
            OtpErlangDouble v = (OtpErlangDouble) cell;
            return new Cell(v.doubleValue());
        }
        else if (cell instanceof OtpErlangAtom)
        {
            OtpErlangAtom v = (OtpErlangAtom) cell;
            return new Cell(v.booleanValue());
        }
        else if (cell instanceof OtpErlangList)
        {
            final OtpErlangList l = (OtpErlangList) cell;
            assert (l.arity() == 0);
            return null;
        }
        else
        {
            throw new InvalidTermToBinaryException("Unknown cell type encountered: " + cell.toString() + ", unable to" +
                                                           " continue parsing.");
        }
    }
}
