package com.basho.riak.client.core.operations;

import com.basho.riak.protobuf.RiakMessageCodes;
import com.basho.riak.protobuf.RiakPB;

import java.util.List;

/**
 * Created by srg on 1/12/16.
 */
public class ToggleTTBEncodingOperation extends PBFutureOperation<ToggleTTBEncodingOperation.Response, RiakPB.RpbToggleEncodingResp, String>
{

    public ToggleTTBEncodingOperation(boolean useTTBEncoding) {
        super(RiakMessageCodes.MSG_ToggleEncodingReq,
                RiakMessageCodes.MSG_ToggleEncodingResp,
                RiakPB.RpbToggleEncodingResp.newBuilder().setUseNative(useTTBEncoding),
                RiakPB.RpbToggleEncodingResp.PARSER);
    }

    @Override
    protected Response convert(List<RiakPB.RpbToggleEncodingResp> responses) {
        // This is not a streaming op, there will only be one response
        checkAndGetSingleResponse(responses);
        return new ToggleTTBEncodingOperation.Response(responses.get(0).getUseNative());
    }

    @Override
    public String getQueryInfo() {
        return "SwitchToUSETTTBOperation";
    }

    /**
     * Response returned from a SwitchToUSETTTBOperation
     */
    public static class Response {
        private final boolean useNativeEncoding;

        public Response(boolean useNativeEncoding) {
            this.useNativeEncoding = useNativeEncoding;
        }

        public boolean isUseNativeEncoding() {
            return useNativeEncoding;
        }
    }
}
