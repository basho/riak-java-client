package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * A wrapper that reads a single element an underlying {@link InputStream}
 * containing contains a delimited list
 * 
 * @author jlee <jonjlee@gmail.com>
 */
public class OneTokenInputStream extends InputStream {
    static final Charset LATIN_1 = Charset.forName("ISO-8859-1");

    int maxBufferLen;
    InputStream impl;
    boolean eof = false;
    int pos = 0;
    int dataLen = 0;
    int bufOffset = 0;
    StringBuilder buf = null;
    String delimiter; 

    public OneTokenInputStream(InputStream in, String delimiter) {
        impl = in;
        maxBufferLen = Math.max(1024, delimiter.length() * 2);
        this.delimiter = delimiter;
    }

    @Override public int read() throws IOException {
        while (!eof && pos >= dataLen) {
            buffer();
        }
        if (pos >= dataLen)
            return -1;

        char c = buf.charAt(pos - bufOffset);
        pos++;
        return c;
    }

    @Override public void close() throws IOException {
        impl.close();
        impl = null;
        eof = true;
    }
    
    public boolean done() {
        return eof;
    }
    
    private void buffer() throws IOException {
        if (eof)
            return;

        if (buf == null) {
            initBuffer();
        }
        if (!eof) {
            int c = impl.read();
            if (c == -1) {
                dataLen = buf.length();
                eof = true;
            } else {
                buf.append((char) c);
                if (buf.length() > maxBufferLen) {
                    bufOffset += buf.length() - delimiter.length();
                    buf.delete(0, buf.length() - delimiter.length());
                }
                if (buf.indexOf(delimiter) >= 0) {
                    eof = true;
                } else {
                    dataLen++;
                }
            }
        }
    }
    
    private void initBuffer() throws IOException {
        byte[] headStart = new byte[delimiter.length() - 1];
        int offset = 0;
        while (offset < headStart.length) {
            int bytesRead = impl.read(headStart, offset, headStart.length - offset);
            if (bytesRead == -1) {
                eof = true;
                return;
            } else {
                offset += bytesRead;
            }
        }
        buf = new StringBuilder(new String(headStart, LATIN_1));
    }
}
