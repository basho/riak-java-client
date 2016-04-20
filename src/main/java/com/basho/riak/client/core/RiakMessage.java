package com.basho.riak.client.core;

import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpInputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the raw bytes sent to or received from Riak.
 * @author Brian Roach <roach at basho dot com>
 * @author Sergey Galkin <sgalkin at basho dot com>
 * @since 2.0
 */
public final class RiakMessage
{
    private static final Logger logger = LoggerFactory.getLogger(RiakMessage.class);
    private final byte code;
    private final byte[] data;
    private final OtpInputStream ttbInputStream;
    private final RiakResponseException riakError;

    public RiakMessage(byte code, byte[] data)
    {
        this.code = code;
        this.data = data;

        switch(this.code) {
            case RiakMessageCodes.MSG_ErrorResp:
                this.ttbInputStream = null;
                this.riakError = getRiakErrorFromPbuf(this.data);
                break;
            case RiakMessageCodes.MSG_TsTtbMsg:
                this.ttbInputStream = new OtpInputStream(data);
                this.riakError = getRiakErrorFromTtb(this.ttbInputStream);
                // Set stream back to the beginning for codec's use
                this.ttbInputStream.setPos(0);
                break;
            default:
                this.riakError = null;
                this.ttbInputStream = null;
        }
    }

    public byte getCode() {
        return code;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isTtbMessage() {
        return this.ttbInputStream != null;
    }

    public boolean isRiakError() {
        return this.riakError != null;
    }

    public OtpInputStream getTtbStream() {
        return this.ttbInputStream;
    }

    public RiakResponseException getRiakError() {
        return this.riakError;
    }

    private static RiakResponseException getRiakErrorFromPbuf(byte[] data) {
        try {
            RiakPB.RpbErrorResp err = RiakPB.RpbErrorResp.parseFrom(data);
            return new RiakResponseException(err.getErrcode(), err.getErrmsg().toStringUtf8());
        } catch (InvalidProtocolBufferException ex) {
            // TODO GH-611
            logger.error("exception", ex);
            return new RiakResponseException(0, "could not parse protocol buffers error");
        }
    }

    private RiakResponseException getRiakErrorFromTtb(OtpInputStream ttbInputStream) {
        int ttbMsgArity = 0;

        try {
            ttbMsgArity = ttbInputStream.read_tuple_head();
        } catch (OtpErlangDecodeException ex) {
            // TODO GH-611
            logger.error("exception", ex);
        }

        if (ttbMsgArity == 3) {
            // NB: may be an error response
            // TODO GH-611 message atom constants?
            String atom = "unknown";
            try {
                atom = ttbInputStream.read_atom();
            } catch (OtpErlangDecodeException ex) {
                // TODO GH-611
                logger.error("exception", ex);
            }

            if ("rpberrorresp".equals(atom)) {
                try {
                    String errMsg = new String(ttbInputStream.read_binary(), StandardCharsets.UTF_8);
                    int errCode = ttbInputStream.read_int(); // TODO GH-611 is errcode an int?
                    return new RiakResponseException(errCode, errMsg);
                } catch (OtpErlangDecodeException ex) {
                    // TODO GH-611
                    logger.error("exception", ex);
                }
            }
        } 

        return null;
    }
}