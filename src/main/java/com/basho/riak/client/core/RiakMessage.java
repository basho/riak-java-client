package com.basho.riak.client.core;

import com.basho.riak.client.core.netty.RiakResponseException;
import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the raw bytes sent to or received from Riak.
 *
 * @author Brian Roach <roach at basho dot com>
 * @author Sergey Galkin <sgalkin at basho dot com>
 * @since 2.0
 */
public final class RiakMessage
{
    private static final Logger logger = LoggerFactory.getLogger(RiakMessage.class);
    private final byte code;
    private final byte[] data;
    private final RiakResponseException riakError;
    private static final String ERROR_RESP = "rpberrorresp";

    public RiakMessage(byte code, byte[] data)
    {
        this(code, data, true);
    }

    public RiakMessage(byte code, byte[] data, boolean doErrorCheck)
    {
        this.code = code;
        this.data = data;

        if(doErrorCheck)
        {
            switch (this.code)
            {
                case RiakMessageCodes.MSG_ErrorResp:
                    this.riakError = getRiakErrorFromPbuf(this.data);
                    break;
                case RiakMessageCodes.MSG_TsTtbMsg:
                    OtpInputStream ttbInputStream = new OtpInputStream(data);
                    this.riakError = getRiakErrorFromTtb(ttbInputStream);
                    break;
                default:
                    this.riakError = null;
            }
        }
        else
        {
            this.riakError = null;
        }
    }

    private static RiakResponseException getRiakErrorFromPbuf(byte[] data)
    {
        try
        {
            RiakPB.RpbErrorResp err = RiakPB.RpbErrorResp.parseFrom(data);
            return new RiakResponseException(err.getErrcode(), err.getErrmsg().toStringUtf8());
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.error("exception", ex);
            return new RiakResponseException(0, "Could not parse protocol buffers error");
        }
    }

    public byte getCode()
    {
        return code;
    }

    public byte[] getData()
    {
        return data;
    }

    public boolean isRiakError()
    {
        return this.riakError != null;
    }

    public RiakResponseException getRiakError()
    {
        return this.riakError;
    }

    private RiakResponseException getRiakErrorFromTtb(OtpInputStream ttbInputStream)
    {
        final String decodeErrorMsg = "Error decoding Riak TTB Response, unexpected format.";
        int ttbMsgArity;

        try
        {
            ttbMsgArity = ttbInputStream.read_tuple_head();
        }
        catch (OtpErlangDecodeException ex)
        {
            logger.error(decodeErrorMsg + " Was expecting a tuple head.", ex);
            throw new IllegalArgumentException(decodeErrorMsg, ex);
        }

        if (ttbMsgArity == 3)
        {
            // NB: may be an error response
            String atom;
            try
            {
                atom = ttbInputStream.read_atom();
            }
            catch (OtpErlangDecodeException ex)
            {
                logger.error(decodeErrorMsg + " Was expecting an atom.", ex);
                throw new IllegalArgumentException(decodeErrorMsg, ex);
            }

            if (ERROR_RESP.equals(atom))
            {
                try
                {
                    String errMsg = new String(ttbInputStream.read_binary(), StandardCharsets.UTF_8);
                    int errCode = ttbInputStream.read_int();
                    return new RiakResponseException(errCode, errMsg);
                }
                catch (OtpErlangDecodeException ex)
                {
                    logger.error(decodeErrorMsg, ex);
                    throw new IllegalArgumentException(decodeErrorMsg, ex);
                }
            }
        }

        return null;
    }
}
