package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.basho.riak.protobuf.RiakTsPB;
import com.ericsson.otp.erlang.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
public class RiakTTBCodec extends ByteToMessageCodec<RiakMessage> {
    private static final OtpErlangAtom undefined = new OtpErlangAtom("undefined");
    private static final OtpErlangAtom tscell = new OtpErlangAtom("tscell");
    private static final OtpErlangAtom tsrow = new OtpErlangAtom("tsrow");
    private static final OtpErlangAtom tsputreq = new OtpErlangAtom("tsputreq");
    private static final OtpErlangString geoCheckin = //new OtpErlangBinary(
            new OtpErlangString("GeoCheckin"); //);

    private final OtpErlangBinary family1 = new OtpErlangBinary(new OtpErlangString("family1"));


    @Override
    protected void encode(ChannelHandlerContext ctx, RiakMessage msg, ByteBuf out) throws Exception {
        final OtpErlangTuple t;

        switch (msg.getCode()) {
            case RiakMessageCodes.MSG_TsPutReq:
                t = encodeTsPut(msg);
                break;
            default:
                throw new IllegalStateException("Can't encode TTB request, unsupported message with code " + msg.getCode());
        }

        final OtpOutputStream os = new OtpOutputStream(t);
        byte data[] = os.toByteArray();

        int length = data.length+1;
        out.writeInt(length);

        /**
         * DO A TOP SECRET HACK
         *
         * It seems that the message is missing the 131 tag required by term_to_binary.
         * As is evident from the Java output, this tag is not being added by jinterface encode.
         * If I simply add 131 to the beginning of the binary it decodes correctly.
         *
         * http://stackoverflow.com/questions/15189447/jinterface-to-create-external-erlang-term
         */
        out.writeByte(131);
        out.writeBytes(data);
    }

    private static OtpErlangTuple pbCellToTtb(RiakTsPB.TsCell c) {
        final OtpErlangObject o[];

        if (c.hasVarcharValue()) {
            o = new OtpErlangObject[]
                    {tscell,
                            new OtpErlangBinary(new OtpErlangString(c.getVarcharValue().toStringUtf8())),
                            undefined, undefined, undefined, undefined};
        } else if (c.hasSint64Value()) {
            o = new OtpErlangObject[]
                    {tscell,
                            undefined,
                            new OtpErlangLong(c.getSint64Value()), undefined, undefined, undefined};
        } else if (c.hasTimestampValue()) {
            o = new OtpErlangObject[]
                    {tscell,
                            undefined, undefined,
                            new OtpErlangLong(c.getTimestampValue()),
                            undefined, undefined};
        } else if (c.hasBooleanValue()) {
            o = new OtpErlangObject[]
                    {tscell,
                            undefined, undefined, undefined,
                            new OtpErlangBoolean(c.getBooleanValue()),
                            undefined};
        } else if (c.hasDoubleValue()) {
            o = new OtpErlangObject[]
                    {tscell,
                            undefined, undefined, undefined, undefined,
                            new OtpErlangDouble(c.getDoubleValue())};
        } else {
            throw new IllegalStateException("Unsupported type");
        }

        return new OtpErlangTuple(o);
    }

    private static OtpErlangTuple pbRowToTtb(RiakTsPB.TsRow r) {
        final OtpErlangObject cells[] = new OtpErlangObject[r.getCellsCount()];
        for (int i = 0; i < cells.length; ++i) {
            final RiakTsPB.TsCell c = r.getCells(i);
            cells[i] = pbCellToTtb(c);
        }

        final OtpErlangList l = new OtpErlangList(cells);
        return new OtpErlangTuple(new OtpErlangObject[] {tsrow, l});
    }

    private static OtpErlangBinary pbStrToTtb(ByteString bs) {
        return new OtpErlangBinary(bs.toByteArray());
    }

    private static OtpErlangTuple encodeTsPut(RiakMessage msg) throws InvalidProtocolBufferException, UnsupportedEncodingException {
        final RiakTsPB.TsPutReq req = RiakTsPB.TsPutReq.parseFrom(msg.getData());
        assert req != null;


        final OtpErlangObject rows[] = new OtpErlangObject[req.getRowsCount()];
        for (int i = 0; i < rows.length; ++i) {
            final RiakTsPB.TsRow r = req.getRows(i);
            rows[i] = pbRowToTtb(r);
        }

        OtpErlangObject[] elems = new OtpErlangObject[]
                {tsputreq, pbStrToTtb(req.getTable()),
                        new OtpErlangList(),
                        new OtpErlangList(rows)};

        return new OtpErlangTuple(elems);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Make sure we have 4 bytes
        if (in.readableBytes() >= 4) {
            in.markReaderIndex();

            int length = in.readInt();
            // See if we have the full frame.
            if (in.readableBytes() < length) {
                in.capacity(length);
                in.resetReaderIndex();
                return;
            }


            final byte[] array = new byte[length];
            in.readBytes(array);

            final OtpErlangObject o;
            try {
                OtpInputStream is = new OtpInputStream(array);
                o = is.read_any();
            }catch (Exception ex){
                in.resetReaderIndex();
                return;
            }

            assert o != null;
            if (o instanceof OtpErlangTuple && ((OtpErlangTuple)o).elementAt(0) instanceof OtpErlangAtom) {
                final OtpErlangTuple t = ((OtpErlangTuple)o);
                final OtpErlangAtom resp = (OtpErlangAtom) ((OtpErlangTuple)o).elementAt(0);
                final String v = resp.atomValue();

                if ("tsputresp".equals(v)) {
                    final RiakTsPB.TsPutResp r = RiakTsPB.TsPutResp.newBuilder().build();
                    out.add(new RiakMessage(RiakMessageCodes.MSG_TsPutResp, r.toByteArray()));
                } else if ("rpberrorresp".equals(v)) {
                    final OtpErlangString errMsg = (OtpErlangString) t.elementAt(1);
                    final OtpErlangLong errCode = (OtpErlangLong) t.elementAt(2);
                    final RiakPB.RpbErrorResp r = RiakPB.RpbErrorResp.newBuilder()
                            .setErrcode(errCode.intValue())
                            .setErrmsg(ByteString.copyFromUtf8(errMsg.stringValue()))
                            .build();
                    out.add(new RiakMessage(RiakMessageCodes.MSG_ErrorResp, r.toByteArray()));
                } else {
                    throw new IllegalStateException("Can't decode TTB response, unsupported atom '"+ v + "'");
                }
            } else {
                throw new IllegalStateException("Can't decode TTB response");
            }
        }
    }
}
