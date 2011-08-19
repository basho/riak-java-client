package com.basho.riak.client.raw.pbc;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.pbc.RiakObject;

import static com.basho.riak.test.util.ExpectedValues.*;

import static junit.framework.Assert.*;

public class TestConversionUtil {

    @Test public void validateUserMetaConverts() throws Exception {
        Map<String, String> userMeta = new HashMap<String, String>();
        userMeta.put("key0", "value0");
        userMeta.put("key1", "value1");

        RiakObject riakObject = new RiakObject(BS_VCLOCK, BS_BUCKET, BS_KEY, BS_CONTENT);
        for (Map.Entry<String, String> entry : userMeta.entrySet()) {
            riakObject.addUsermetaItem(entry.getKey(), entry.getValue());
        }

        IRiakObject converted = ConversionUtil.convert(riakObject);
        assertEquals(userMeta, converted.getMeta());
    }

}
