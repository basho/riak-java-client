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
package com.basho.riak.client.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that can be branched into other InputStreams, each
 * maintaining its own location.
 * 
 * @author jlee <jonjlee@gmail.com>
 */
public class BranchableInputStream extends InputStream {

    static final int DEFAULT_BASE_CHUNK_SIZE = 1024;
    static final int MAX_BYTES_PER_READ = 1024;
    int nextChunkSize;

    InputStream impl;
    InputStreamBranch mainBranch = null;
    LinkedChunk lastChunk = null;
    int dataLen = 0;
    boolean eof = false;

    public BranchableInputStream(InputStream in) {
        this(in, DEFAULT_BASE_CHUNK_SIZE);
    }

    public BranchableInputStream(InputStream in, int initialBufferSize) {
        impl = in;
        lastChunk = new LinkedChunk(0, 0); 
        mainBranch = new InputStreamBranch(lastChunk, 0);
        nextChunkSize = initialBufferSize;
    }

    @Override public int read() throws IOException {
        return mainBranch.read();
    }

    @Override public void close() throws IOException {
        mainBranch.close();
        impl.close();
    }

    public InputStream branch() {
        return new InputStreamBranch(mainBranch);
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
        
        return pos < dataLen;
    }

    class InputStreamBranch extends InputStream {

        LinkedChunk chunk;
        int pos;

        InputStreamBranch(LinkedChunk chunk, int pos) {
            this.chunk = chunk;
            this.pos = pos;
        }

        InputStreamBranch(InputStreamBranch other) {
            this.chunk = other.chunk;
            this.pos = other.pos;
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
    
    class LinkedChunk {
        int offset;
        int len;
        byte[] buf;
        LinkedChunk next = null;
        LinkedChunk(int offset, int size) {
            this.offset = offset;
            this.buf = new byte[size];
            this.len = 0;
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
