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
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An input stream that can be branched into other InputStreams, each
 * maintaining its own location.
 * 
 * @author jlee <jonjlee@gmail.com>
 */
public class BranchableInputStream extends InputStream {

    static final int DEFAULT_BUFFER_SIZE = 4096;
    static final int READ_CHUNK_SIZE = 1024;

    int readOffset = 0;
    int bufOffset = 0;
    int bufLen = 0;
    volatile byte buf[];
    Map<InputStreamBranch, Integer> branches = new LinkedHashMap<InputStreamBranch, Integer>();
    InputStream impl;

    public BranchableInputStream(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    public BranchableInputStream(InputStream in, int initialBufferSize) {
        impl = in;
        buf = new byte[initialBufferSize];
    }

    @Override public int read() throws IOException {
        if (buf == null)
            return -1;
        int c = readAt(readOffset);
        readOffset++;
        return c;
    }

    @Override public void close() throws IOException {
        buf = null;
        impl.close();
    }

    public InputStream branch() {
        return new InputStreamBranch();
    }

    public int readAt(int pos) throws IOException {
        readUntil(pos);
        pos -= bufOffset;
        if (pos >= 0 && pos < bufLen)
            return buf[pos] & 0xff;
        return -1;
    }

    void readUntil(int pos) throws IOException {
        int bytesRead = 0;
        while ((pos - bufOffset >= bufLen) && (bytesRead >= 0)) {
            ensureBuffer(bufOffset + bufLen + READ_CHUNK_SIZE);
            bytesRead = impl.read(buf, bufLen, READ_CHUNK_SIZE);
            if (bytesRead > 0) {
                bufLen += bytesRead;
            }
        }
    }

    void ensureBuffer(int size) {
        if (!needToReallocate(size))
            return;

        // We only need to keep a buffer containing data back to the
        // earliest read location in all branches
        int offset = readOffset;
        for (int i : branches.values()) {
            if (i < offset) {
                offset = i;
            }
        }
        size -= offset;

        if (size > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        int bufSize = DEFAULT_BUFFER_SIZE;
        while (size > bufSize) {
            if (bufSize > Integer.MAX_VALUE / 2) {
                // guaranteed not to overflow, since DEFAULT_BUFFER_SIZE is a
                // power of 2.
                bufSize += Integer.MAX_VALUE / 4;
            } else {
                bufSize *= 2;
            }
        }

        reallocateBuffer(offset, bufSize);
    }

    void reallocateBuffer(int offset, int size) {
        if (bufOffset == offset && buf.length == size)
            return;

        if (offset + size < bufOffset + bufLen)
            throw new BufferUnderflowException();

        byte[] copy = new byte[size];
        if (offset != bufOffset) {
            if (offset - bufOffset < buf.length) {
                bufLen -= (offset - bufOffset);
                System.arraycopy(buf, offset - bufOffset, copy, 0, bufLen);
            } else {
                bufLen = 0;
            }

            bufOffset = offset;
        } else {
            System.arraycopy(buf, 0, copy, 0, bufLen);
        }
        buf = copy;
    }

    boolean needToReallocate(int size) {
        return (buf.length < size - bufOffset);
    }

    class InputStreamBranch extends InputStream {

        int offset;
        boolean closed = false;

        public InputStreamBranch() {
            branches.put(this, readOffset);
            offset = readOffset;
        }

        @Override public int read() throws IOException {
            if (closed)
                return -1;
            int b = readAt(offset++);
            branches.put(this, offset);
            return b;
        }

        @Override public void close() {
            branches.remove(this);
            closed = true;
        }
    }
}
