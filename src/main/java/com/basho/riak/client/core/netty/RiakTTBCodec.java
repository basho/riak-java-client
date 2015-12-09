package com.basho.riak.client.core.netty;

import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakTsPB;
import com.ericsson.otp.erlang.*;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.util.List;
import java.util.concurrent.Exchanger;

/**
 * Created by srg on 12/9/15.
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
        switch (msg.getCode()) {
            case RiakMessageCodes.MSG_TsPutReq:
                encodeTsPut(msg, out);
                break;
            default:
                throw new IllegalStateException("Unsupported message with code " + msg.getCode());
        }
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

    private static OtpErlangTuple encodeTsPut(RiakMessage msg, ByteBuf out) throws InvalidProtocolBufferException {
        final RiakTsPB.TsPutReq req = RiakTsPB.TsPutReq.parseFrom(msg.getData());
        assert req != null;


        final OtpErlangObject rows[] = new OtpErlangObject[req.getRowsCount()];
        for (int i = 0; i < rows.length; ++i) {
            final RiakTsPB.TsRow r = req.getRows(i);
            rows[i] = pbRowToTtb(r);
        }

        OtpErlangObject[] elems = new OtpErlangObject[]
                {tsputreq, new OtpErlangString(req.getTable().toStringUtf8()),
                        new OtpErlangList(),
                        new OtpErlangList(rows)};

        final OtpErlangTuple request = new OtpErlangTuple(elems);


        OtpOutputStream os = new OtpOutputStream(request);
        byte data[] = os.toByteArray();

        int length = data.length;
        out.writeInt(length);
        out.writeBytes(data);

        return request;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Make sure we have 4 bytes
        if (in.readableBytes() >= 4) {
            in.markReaderIndex();

            int i = in.readableBytes();

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





//            // See if we have the full frame.
//            if (in.readableBytes() < length) {
//                in.resetReaderIndex();
//                return;
//            } else {
//                byte code = in.readByte();
//                byte[] array = new byte[length - 1];
//                in.readBytes(array);
//                out.add(new RiakMessage(code, array));
//            }

            assert o != null;
        }
    }
}
