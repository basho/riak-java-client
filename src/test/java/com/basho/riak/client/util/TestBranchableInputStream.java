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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.basho.riak.client.util.BranchableInputStream.InputStreamBranch;
import com.basho.riak.client.util.BranchableInputStream.LinkedChunk;

public class TestBranchableInputStream {

    BranchableInputStream impl;
    
    @Test public void behaves_as_standard_input_stream_for_short_inputs() throws IOException {
        byte[] bytes = "short buffer".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        impl = new BranchableInputStream(is);
        ClientUtils.copyStream(impl, os);
        
        assertArrayEquals(bytes, os.toByteArray());
    }
    
    @Test public void behaves_as_standard_input_stream_for_long_inputs() throws IOException {
        byte[] bytes = new byte[BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 5];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        impl = new BranchableInputStream(is);
        ClientUtils.copyStream(impl, os);
        
        assertArrayEquals(bytes, os.toByteArray());
    }
    
    @Test public void read_array_works() throws IOException {
        byte[] bytes = new byte[100];
        byte[] copy = new byte[100];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        
        impl = new BranchableInputStream(is);
        impl.read(copy);
        
        assertArrayEquals(bytes, copy);
    }
    
    @Test public void all_branches_read_data() throws IOException {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        InputStreamBranch[] branches = new InputStreamBranch[5];
        impl = new BranchableInputStream(is);
        
        for (int i = 0; i < 5; i++) {
            branches[i] = (InputStreamBranch) impl.branch();
            int c = impl.read();
            assertEquals(bytes[i], (byte) c);
        }
        
        for (int i = 0; i < 5; i++) {
            for (int j = i; j < bytes.length; j++) {
                assertEquals(bytes[j], (byte) branches[i].read());
            }
        }

        assertEquals(-1, impl.read());
    }

    @Test public void read_position_updates_when_reading() throws IOException {
        byte[] bytes = "012345678901234567890123456789".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        impl = new BranchableInputStream(is);
        
        for (int i = 0; i < 20; i++) {
            assertEquals(i, impl.pos);
            impl.read();
        }   
    }

    @Test public void primary_and_branch_positions_updates_when_reading_from_branch() throws IOException {
        byte[] bytes = "012345678901234567890123456789".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        InputStreamBranch[] branches = new InputStreamBranch[5];
        impl = new BranchableInputStream(is);
        
        
        for (int i = 0; i < 5; i++) {
            branches[i] = (InputStreamBranch) impl.branch();
            impl.read();
        }
        
        for (int i = 0; i < 5; i++) {
            for (int j = i; j < 20; j++) {
                assertEquals(j, branches[i].pos);
                branches[i].read();
            }
        }

        assertEquals(20, impl.pos);
    }
    
    @Test public void no_data_loss_when_branches_span_chunks() throws IOException {
        byte[] bytes = new byte[BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 5];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        impl = new BranchableInputStream(is);

        InputStream[] branches = new InputStreamBranch[2];
        branches[0] = impl.branch();
        
        for (int i = 0; i < BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 3; i++) {
            impl.read();
        }   

        branches[1] = impl.branch();
        for (int i = 0; i < BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE; i++) {
            branches[1].read();
        }   

        assertEquals(BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 4, impl.pos);

        os.reset();
        ClientUtils.copyStream(branches[0], os);
        assertArrayEquals(bytes, os.toByteArray());

        os.reset();
        ClientUtils.copyStream(branches[1], os);
        assertArrayEquals(Arrays.copyOfRange(bytes, BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 4, BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 5), os.toByteArray());

        assertEquals(-1, impl.read());
    }
    
    @Test public void chunk_sizes_increase_exponentially() throws IOException {
        byte[] bytes = new byte[BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE * 5];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        
        impl = new BranchableInputStream(is);
        LinkedChunk firstChunk = impl.lastChunk;
        InputStream branch = impl.branch();
        ClientUtils.copyStream(branch, os);
        
        assertEquals(0, firstChunk.buf.length);
        assertEquals(BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE, firstChunk.next().buf.length);
        assertEquals(BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE*2, firstChunk.next().next().buf.length);
        assertEquals(BranchableInputStream.DEFAULT_BASE_CHUNK_SIZE*4, firstChunk.next().next().next().buf.length);

        assertTrue(firstChunk.full());
        assertTrue(firstChunk.next().full());
        assertTrue(firstChunk.next().next().full());
        assertFalse(firstChunk.next().next().next().full());

        assertNull(firstChunk.next().next().next().next());
    }
}
