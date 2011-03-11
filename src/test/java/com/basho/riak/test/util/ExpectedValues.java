/**
 * 
 */
package com.basho.riak.test.util;

import static com.google.protobuf.ByteString.copyFromUtf8;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.client.util.Constants;
import com.basho.riak.pbc.RPB.RpbLink;
import com.basho.riak.pbc.RPB.RpbPair;
import com.google.protobuf.ByteString;

/**
 * Some constants for test values that crop up time and again.
 * 
 * @author russell
 * 
 */
public final class ExpectedValues {

    public static final String BUCKET = "bucket";
    public static final String KEY = "key";
    public static final String CONTENT = "content";
    public static final String VCLOCK = "a85hYGBgymDKBVIsrLnh3BlMiYx5rAymfeeO8EGFWRLl30GF/00ACmcBAA==";
    public static final String TAG = "tag";
    public static final String JSON_CTYPE = Constants.CTYPE_JSON;
    public static final String UTF8_CHARSET = "UTF-8";
    public static final String GZIP_ENC = "gzip";
    public static final String VTAG = "16vic4eU9ny46o4KPiDz1f";
    public static final int LAST_MOD = 1271442363;
    public static final int LAST_MOD_USEC = 105696; 

    public static final ByteString BS_BUCKET = copyFromUtf8(BUCKET);
    public static final ByteString BS_KEY = copyFromUtf8(KEY);
    public static final ByteString BS_CONTENT = copyFromUtf8(CONTENT);
    public static final ByteString BS_VCLOCK = copyFromUtf8(VCLOCK);
    public static final ByteString BS_TAG = copyFromUtf8(TAG);
    public static final ByteString BS_JSON_CTYPE = copyFromUtf8(JSON_CTYPE);
    public static final ByteString BS_UTF8_CHARSET = copyFromUtf8(UTF8_CHARSET);
    public static final ByteString BS_GZIP_ENC = copyFromUtf8(GZIP_ENC);
    public static final ByteString BS_VTAG = copyFromUtf8(VTAG);

    private ExpectedValues() {}

    /**
     * Generate a number of RpbLinks, each link has the values
     * {@link ExpectedValues#BUCKET} _ <i>n</i>, {@link ExpectedValues#KEY} _ <i>n</i>, {@link ExpectedValues#TAG} _ <i>n</i>
     * @param num how many RpbLinks to generate
     * @return List of RpbLinks
     */
    public static List<RpbLink> rpbLinks(int num) {
        final int numLinks = num;
        final List<RpbLink> rpbLinks = new ArrayList<RpbLink>();
        
        for(int i=0; i < numLinks; i++) {
            RpbLink.Builder builder = RpbLink.newBuilder()
                .setBucket(concatToByteString(BUCKET, i))
                .setKey(concatToByteString(KEY, i))
                .setTag(concatToByteString(TAG, i));
            rpbLinks.add(builder.build());
        }
        
        return rpbLinks;
    }
    
    
    public static ByteString concatToByteString(String value, int counter) {
        return ByteString.copyFromUtf8(value + "_" + counter);
    }

    /**
     * Create a List of RpbPairs from a pair of String arrays.
     * keys and values must be of the same length.
     * 
     * @param keys 
     * @param values
     * @return List of RpbPairs one for each key, value pair
     */
    public static List<RpbPair> rpbPairs(String[] keys, String[] values) {
        if(keys.length != values.length) {
            throw new IllegalArgumentException("keys and values must be same length"); 
        }
        
        final List<RpbPair> pairs = new ArrayList<RpbPair>();
        
        for(int i=0; i < keys.length; i++ ) {
            RpbPair.Builder builder = RpbPair.newBuilder();
            builder.setKey(copyFromUtf8(keys[i]));
            builder.setValue(copyFromUtf8(values[i]));
            pairs.add(builder.build());
        }
        
        return pairs;
    }
    

}
