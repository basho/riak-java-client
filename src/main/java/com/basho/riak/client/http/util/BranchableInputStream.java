/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.http.util;

import java.io.IOException;

import java.io.InputStream;

/**
 * An input stream that can be branched into other InputStreams, each
 * maintaining its own location, with the main read() method always returning
 * bytes from the furthest advanced branch.
 * 
 * @author jlee <jonjlee@gmail.com>
 */
public class BranchableInputStream extends InputStream {

    static final int DEFAULT_BASE_CHUNK_SIZE = 1024;
    static final int MAX_BYTES_PER_READ = 1024;
    int nextChunkSize;

    InputStream impl;
    LinkedChunk lastChunk = null;
    int dataLen = 0;
    int pos;
    boolean eof = false;

    public BranchableInputStream(InputStream in) {
        this(in, DEFAULT_BASE_CHUNK_SIZE);
    }

    public BranchableInputStream(InputStream in, int initialBufferSize) {
        impl = in;
        lastChunk = new LinkedChunk(0, 0);
        nextChunkSize = initialBufferSize;
    }

    @Override public int read() throws IOException {
        int curpos = pos;
        if (readUntil(curpos))
            return lastChunk.get(curpos);
        return -1;
    }

    @Override public void close() throws IOException {
        eof = true;
        impl.close();
    }

    public int peek() throws IOException {
        int curpos = pos;
        int c = read();
        pos = curpos;
        return c;
    }

    public InputStream branch() {
        return new InputStreamBranch(lastChunk, pos);
    }

    boolean readUntil(int pos) throws IOException {
        if (!eof) {
            while ((pos >= dataLen) && !eof) {
                if (lastChunk.full()) {
                    lastChunk.setNext(new LinkedChunk(lastChunk.lastIndex() + 1, nextChunkSize));
                    lastChunk = lastChunk.next();
                    nextChunkSize *= 2;
                }

                int bytesRead = lastChunk.readFrom(impl, MAX_BYTES_PER_READ);
                if (bytesRead < 0) {
                    eof = true;
                } else {
                    dataLen += bytesRead;
                }
            }
        }
        if (pos < dataLen) {
            this.pos = Math.max(this.pos, pos + 1);
            return true;
        }
        return false;
    }

    class InputStreamBranch extends InputStream {

        LinkedChunk chunk;
        int pos;

        InputStreamBranch(LinkedChunk chunk, int pos) {
            this.chunk = chunk;
            this.pos = pos;
        }

        @Override public int read() throws IOException {
            if (chunk == null || !readUntil(pos))
                return -1;

            while (pos > chunk.lastIndex()) {
                chunk = chunk.next;
            }
            return chunk.get(pos++);
        }

        @Override public void close() {
            chunk = null;
        }
    }
    
    //Visible for test.
    protected static class LinkedChunk {
        int offset;
        int len;
        byte[] buf;
        LinkedChunk next = null;

        LinkedChunk(int offset, int size) {
            this.offset = offset;
            buf = new byte[size];
            len = 0;
        }

        int readFrom(InputStream in, int maxBytes) throws IOException {
            int bytesRead = in.read(buf, len, Math.min(remaining(), maxBytes));
            if (bytesRead > 0) {
                len += bytesRead;
            }
            return bytesRead;
        }

        int get(int index) {
            if ((index < offset) || (index - offset >= len))
                return -1;
            return buf[index - offset] & 0xff;
        }

        int lastIndex() {
            return offset + buf.length - 1;
        }

        boolean full() {
            return (len == buf.length);
        }

        int remaining() {
            return (buf.length - len);
        }

        LinkedChunk next() {
            return next;
        }

        void setNext(LinkedChunk next) {
            this.next = next;
        }
    }
}
