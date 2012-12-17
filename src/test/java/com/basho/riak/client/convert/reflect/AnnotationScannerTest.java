package com.basho.riak.client.convert.reflect;


import com.basho.riak.client.RiakLink;
import com.basho.riak.client.convert.*;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class AnnotationScannerTest {

    @Test
    public void testBasicClass() throws Exception {
        AnnotationScanner scanner = new AnnotationScanner(BasicClass.class);
        AnnotationInfo info = scanner.call();
        BasicClass instance = new BasicClass();
        assertEquals("keyValue", info.getRiakKey(instance));
        assertEquals("clock", info.getRiakVClock(instance).asString());
        assertEquals(1, info.getUsermetaData(instance).size());
        assertEquals(1, info.getIndexes(instance).getBinIndex("myBinIndex").size());
        assertEquals(1, info.getIndexes(instance).getIntIndex("myIntIndex").size());
        assertEquals(1, info.getLinks(instance).size());
    }

    public class BasicClass {

        @RiakKey
        private String key = "keyValue";
        @RiakVClock
        private byte[] vClock = "clock".getBytes();
        @RiakUsermeta
        private Map<String, String> usermetaData = new HashMap<String, String>();
        @RiakIndex(name="myBinIndex")
        private String stringIndex = "indexValue";
        @RiakIndex(name="myIntIndex")
        private int intIndex = 3;
        @RiakLinks
        private Collection<RiakLink> links = new HashSet<RiakLink>();

        public BasicClass() {
            usermetaData.put("foo", "bar");
            links.add(new RiakLink("foo", "foo", "foo"));
        }

    }

    @Test
    public void testSimpleInheritance() throws Exception {
        AnnotationScanner scanner = new AnnotationScanner(ChildClass.class);
        AnnotationInfo info = scanner.call();
        ChildClass instance = new ChildClass();
        assertEquals("keyValue", info.getRiakKey(instance));
        assertEquals("clock", info.getRiakVClock(instance).asString());
    }

    public class ChildClass extends ParentClass {
        @RiakVClock
        private byte[] vClock = "clock".getBytes();
    }

    public class ParentClass {
        @RiakKey
        private String key = "keyValue";
    }

}
