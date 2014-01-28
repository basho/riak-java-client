package com.basho.riak.client.convert.reflect;


import com.basho.riak.client.convert.*;
import com.basho.riak.client.query.links.RiakLink;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AnnotationScannerTest {

    @Test
    public void testBasicClass() throws Exception {
        AnnotationScanner scanner = new AnnotationScanner(BasicClass.class);
        AnnotationInfo info = scanner.call();
        BasicClass instance = new BasicClass();
        assertEquals("keyValue", info.getRiakKey(instance));
        assertEquals("clock", info.getRiakVClock(instance).asString());
        assertEquals(1, info.getUsermetaData(instance).size());
        assertEquals(1, info.getIndexes(instance).size());
        assertEquals(1, info.getIndexes(instance).size());
        assertEquals(1, info.getLinks(instance).size());
        
        scanner = new AnnotationScanner(MethodClass.class);
        info = scanner.call();
        MethodClass mInstance = new MethodClass();
        info.setRiakKey(mInstance, "keyValue");
        assertEquals("keyValue", info.getRiakKey(mInstance));
    }

    @Test
    public void testMethodClass() throws Exception {
        AnnotationScanner scanner = new AnnotationScanner(MethodClass.class);
        AnnotationInfo info = scanner.call();
        MethodClass mInstance = new MethodClass();
        info.setRiakKey(mInstance, "keyValue");
        assertEquals("keyValue", info.getRiakKey(mInstance));
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

    public class MethodClass {
        
        private String key;
        
        @RiakKey
        public void setKey(String key) {
            this.key = key;
        }
        
        @RiakKey
        public String getKey() {
            return key;
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
