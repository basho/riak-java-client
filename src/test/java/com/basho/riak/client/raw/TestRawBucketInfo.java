package com.basho.riak.client.raw;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.junit.Test;

import com.basho.riak.client.util.Constants;

public class TestRawBucketInfo {

    @Test public void allow_mult_sets_schema() throws JSONException {
        final boolean ALLOW_MULT = true;
        
        RawBucketInfo impl = new RawBucketInfo();
        impl.setAllowMult(ALLOW_MULT);
        
        assertEquals(ALLOW_MULT, impl.getAllowMult());
        assertEquals(ALLOW_MULT, impl.getSchema().getBoolean(Constants.RAW_FL_SCHEMA_ALLOW_MULT));
    }

    @Test public void n_val_sets_schema() throws JSONException {
        final int N_VAL = 10;
        
        RawBucketInfo impl = new RawBucketInfo();
        impl.setNVal(N_VAL);
        
        assertEquals(N_VAL, (int) impl.getNVal());
        assertEquals(N_VAL, impl.getSchema().getInt(Constants.RAW_FL_SCHEMA_NVAL));
    }

    @Test public void link_fun_sets_schema() throws JSONException {
        final String LINK_MOD = "link_mod";
        final String LINK_FUN = "link_fun";
        
        RawBucketInfo impl = new RawBucketInfo();
        impl.setLinkFun(LINK_MOD, LINK_FUN);
        
        assertEquals(LINK_MOD + ":" + LINK_FUN, impl.getLinkFun());
        assertEquals(LINK_MOD, impl.getSchema().getJSONObject(Constants.RAW_FL_SCHEMA_LINKFUN).getString(Constants.RAW_FL_SCHEMA_LINKFUN_MOD));
        assertEquals(LINK_FUN, impl.getSchema().getJSONObject(Constants.RAW_FL_SCHEMA_LINKFUN).getString(Constants.RAW_FL_SCHEMA_LINKFUN_FUN));
    }

    @Test public void chash_fun_sets_schema() throws JSONException {
        final String HASH_MOD = "hash_mod";
        final String HASH_FUN = "hash_fun";
        
        RawBucketInfo impl = new RawBucketInfo();
        impl.setCHashFun(HASH_MOD, HASH_FUN);
        
        assertEquals(HASH_MOD + ":" + HASH_FUN, impl.getCHashFun());
        assertEquals(HASH_MOD, impl.getSchema().getJSONObject(Constants.RAW_FL_SCHEMA_CHASHFUN).getString(Constants.RAW_FL_SCHEMA_CHASHFUN_MOD));
        assertEquals(HASH_FUN, impl.getSchema().getJSONObject(Constants.RAW_FL_SCHEMA_CHASHFUN).getString(Constants.RAW_FL_SCHEMA_CHASHFUN_FUN));
    }
}
