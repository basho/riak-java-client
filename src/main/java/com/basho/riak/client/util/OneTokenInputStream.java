package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper that reads a single element an underlying {@link InputStream}
 * containing contains a delimited list
 * 
 * @author jlee <jonjlee@gmail.com>
 */
public class OneTokenInputStream extends InputStream {

    int maxBufferLen;
    InputStream impl;
    boolean eof = false;
    int pos = 0;
    int dataLen = 0;
    int bufOffset = 0;
    StringBuilder buf;
    String delimiter; 

    public OneTokenInputStream(InputStream in, String delimiter) {
        impl = in;
        maxBufferLen = Math.max(1024, delimiter.length() * 2);
        this.delimiter = delimiter;
    }

    @Override public int read() throws IOException {
        if (!eof)
            buffer();
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
        if (eof) {
            return;
        }
        if (buf.length() == 0) {
            byte[] headStart = new byte[delimiter.length() - 1];
            int offset = 0;
            while (true) {
                int bytesRead = impl.read(headStart, offset, headStart.length - offset);
                if (bytesRead == -1) {
                    eof = true;
                    break;
                } else {
                    offset += bytesRead;
                }
            }
            buf.append(new String(headStart), 0, offset);
        }
        while (!eof) {
            int c = impl.read();
            if (c == -1) {
                eof = true;
            } else {
                buf.append((char) c);
                if (buf.length() > maxBufferLen) {
                    buf.delete(0, buf.length() - delimiter.length());
                    bufOffset += delimiter.length();
                }
                if (buf.indexOf(delimiter) >= 0) {
                    eof = true;
                } else {
                    dataLen++;
                }
            }
        }
    }
}
