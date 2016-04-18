package com.basho.riak.client.core.codec;

import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpExternal;
import com.ericsson.otp.erlang.OtpOutputStream;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TermToBinaryCodec
{
    private final static Logger logger = LoggerFactory.getLogger(TermToBinaryCodec.class);

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

        public static final OtpErlangAtom tsPutReq = new OtpErlangAtom("tsputreq");
        public static final OtpErlangAtom tsPutResp = new OtpErlangAtom("tsputresp");

        public static final OtpErlangAtom tsDelReq = new OtpErlangAtom("tsdelreq");
        public static final OtpErlangAtom tsDelResp = new OtpErlangAtom("tsdelresp");

        public static final OtpErlangAtom tsUndefined = new OtpErlangAtom("undefined");
    }

    public static byte[] encodeTsGetRequest(String tableName, Collection<Cell> keyValues, Long timeout)
    {
        final OtpOutputStream os = new OtpOutputStream();
        os.write(OtpExternal.versionTag);

        // NB: TsGetReq is a 4-tuple: tsgetreq, tableName, [key values], timeout
        os.write_tuple_head(4);
        os.write_any(Messages.tsGetReq);
        os.write_string(tableName);

        os.write_list_head(keyValues.size());
        for (Cell k : keyValues) {
            os.write_any(k.getErlangObject());
        }
        os.write_nil(); // NB: finishes the list

        // TODO GH-611 timeout?
        os.write_any(Messages.tsUndefined);

        return os.toByteArray();
    }

    public static QueryResult decodeTsGetResponse(byte[] response)
    {
        // fill me in
        return null;
    }

    /*
     Query

         Content = #tsinterpolation{
                      base           = iolist_to_binary(QueryText),
                      interpolations = serialize_interpolations(Interpolations)},
         Msg0 = #tsqueryreq{query = Content},
         Msg1 = Msg0#tsqueryreq{cover_context = Cover},
         Msg = {Msg1, {msgopts, Options}},
     */

    public static byte[] encodeTsQueryRequest(String queryText)
    {
        // fill me in
        return null;
    }

    public static QueryResult decodeTsQueryResponse(byte[] response)
    {
        // fill me in
        return null;
    }

    /*
     Put
         Measurements = [{...},{...}].
         Message = #tsputreq{table   = iolist_to_binary(TableName),
                             columns = [],
                             rows    = Measurements};
         Msg = {Message, {msgopts, Options}},
     */

    public static byte[] encodeTsPutRequest(String tableName, Iterable<Row> rows)
    {
        // fill me in
        return null;
    }

    public static Void decodeTsPutResponse(byte[] response)
    {
        // Do we return anything in TTB?
        return null;
    }

    /*
     Delete

         Message = #tsdelreq{table   = iolist_to_binary(Table),
                        key     = riak_pb_ts_codec:encode_cells_non_strict(Key),
                        vclock  = proplists:get_value(vclock, Options),
                        timeout = proplists:get_value(timeout, Options)},
     */

    public static byte[] encodeTsDeleteRequest(String tableName, Iterable<Cell> keyValues, byte[] vclock, Long timeout)
    {
        // fill me in
        return null;
    }

    public static Void decodeTsDeleteResponse(byte[] response)
    {
        // Do we return anything in TTB?
        return null;
    }
}
