package com.basho.riak.client.util;

import static org.junit.Assert.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class TestOneTokenInputStream {

    OneTokenInputStream impl;
    
    @Test public void stream_ends_at_delimiter_in_middle_of_stream() throws IOException {
        String delim = "\n--boundary";
        String part1 = "abcdefghijklmnop";
        String part2 = "qrstuvwxyz";
        String body = 
            part1 +
    		delim + "\n" +
    		part2;
        InputStream stream = new ByteArrayInputStream(body.getBytes());
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }

    @Test public void no_content_if_stream_starts_with_delimiter() throws IOException {
        String delim = "\n--boundary";
        String body = delim + "abcdef";
        InputStream stream = new ByteArrayInputStream(body.getBytes());
        impl = new OneTokenInputStream(stream, delim);
        
        assertEquals(-1, impl.read());
    }

    @Test public void stream_ends_at_delimiter_at_end_of_stream() throws IOException {
        String delim = "\n--boundary";
        String part1 = "abcdefghijklmnop";
        String body = part1 + delim;
        InputStream stream = new ByteArrayInputStream(body.getBytes());
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }

    @Test public void stream_ends_at_end_of_stream_when_no_delimiter() throws IOException {
        String delim = "\n--boundary";
        String part1 = "abcdefghijklmnop";
        String body = part1;
        InputStream stream = new ByteArrayInputStream(body.getBytes());
        impl = new OneTokenInputStream(stream, delim);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ClientUtils.copyStream(impl, os);
        assertEquals(part1, os.toString());
    }
}
