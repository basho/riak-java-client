package com.basho.riak.client.util;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.junit.Test;

import com.basho.riak.client.util.BranchableInputStream.InputStreamBranch;

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
        byte[] bytes = new byte[BranchableInputStream.DEFAULT_BUFFER_SIZE * 5];
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

        for (int i = 5; i < bytes.length; i++) {
            assertEquals(bytes[i], (byte) impl.read());
        }
    }

    @Test public void read_position_updates_when_reading() throws IOException {
        byte[] bytes = "012345678901234567890123456789".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        impl = new BranchableInputStream(is);
        
        for (int i = 0; i < 20; i++) {
            assertEquals(i, impl.readOffset);
            impl.read();
        }   
    }

    @Test public void branch_position_update_when_reading_from_branch() throws IOException {
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
                assertEquals(j, branches[i].offset);
                branches[i].read();
            }
        }

        assertEquals(5, impl.readOffset);
    }
    
    @Test public void ensureBuffer_readjusts_offset_to_earliest_branch_location() throws IOException {
        byte[] bytes = new byte[BranchableInputStream.DEFAULT_BUFFER_SIZE * 5];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        impl = spy(new BranchableInputStream(is));
        
        when(impl.needToReallocate(anyInt())).thenReturn(true);
        
        for (int i = 0; i < 20; i++) { impl.read(); }
        
        impl.ensureBuffer(1024);
        assertEquals(20, impl.bufOffset);
        
        InputStream b1 = impl.branch();
        for (int i = 0; i < 20; i++) { impl.read(); }
        InputStream b2 = impl.branch();
        for (int i = 0; i < 20; i++) { impl.read(); }
        
        impl.ensureBuffer(1024);
        assertEquals(20, impl.bufOffset);
        
        b1.close();
        impl.ensureBuffer(1024);
        assertEquals(40, impl.bufOffset);

        b2.close();
        impl.ensureBuffer(1024);
        assertEquals(60, impl.bufOffset);
    }
    
    @Test public void ensureBuffer_defers_to_reallocateBuffer() {
        byte[] bytes = new byte[100];
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);

        impl = spy(new BranchableInputStream(is));
        when(impl.needToReallocate(anyInt())).thenReturn(true);
        
        impl.ensureBuffer(1024);
        verify(impl).reallocateBuffer(eq(0), anyInt());
    }

    @Test public void reallocateBuffer_doesnt_lose_data() throws IOException {
        byte[] bytes = new byte[100];
        byte[] copy = new byte[bytes.length];
        new Random().nextBytes(bytes);
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        impl = spy(new BranchableInputStream(is));
        impl.read(copy);
        
        int offset = 0;
        impl.reallocateBuffer(offset, 1024);
        assertEquals(bytes.length, impl.bufLen + offset);
        for (int i = offset; i < bytes.length; i++) {
            assertEquals(bytes[i], impl.buf[i]);
        }

        offset = 10;
        impl.reallocateBuffer(offset, 1024);
        assertEquals(bytes.length, impl.bufLen + offset);
        for (int i = offset; i < bytes.length; i++) {
            assertEquals(bytes[i], impl.buf[i - offset]);
        }

        offset = 20;
        impl.reallocateBuffer(offset, 1024);
        assertEquals(bytes.length, impl.bufLen + offset);
        for (int i = offset; i < bytes.length; i++) {
            assertEquals(bytes[i], impl.buf[i - offset]);
        }

        offset = 100;
        impl.reallocateBuffer(offset, 1024);
        assertEquals(0, impl.bufLen);
    }

    @Test public void ensureBuffer_allocates_enough_space() {
        impl = spy(new BranchableInputStream(null));
        when(impl.needToReallocate(anyInt())).thenReturn(true);
        doNothing().when(impl).reallocateBuffer(anyInt(), anyInt());
        
        impl.ensureBuffer(100);
        verify(impl).reallocateBuffer(eq(0), geq(100));
        reset(impl);
        
        when(impl.needToReallocate(anyInt())).thenReturn(true);
        doNothing().when(impl).reallocateBuffer(anyInt(), anyInt());
        impl.ensureBuffer(BranchableInputStream.DEFAULT_BUFFER_SIZE);
        verify(impl).reallocateBuffer(eq(0), geq(BranchableInputStream.DEFAULT_BUFFER_SIZE)); 
        reset(impl);

        when(impl.needToReallocate(anyInt())).thenReturn(true);
        doNothing().when(impl).reallocateBuffer(anyInt(), anyInt());
        impl.ensureBuffer(BranchableInputStream.DEFAULT_BUFFER_SIZE * 10);
        verify(impl).reallocateBuffer(eq(0), geq(BranchableInputStream.DEFAULT_BUFFER_SIZE * 10)); 
        reset(impl);
    }

    @Test public void ensureBuffer_allocates_exponentially() {
        impl = spy(new BranchableInputStream(null));
        doNothing().when(impl).reallocateBuffer(anyInt(), anyInt());
        when(impl.needToReallocate(anyInt())).thenReturn(true);
        
        impl.ensureBuffer(100);
        verify(impl).reallocateBuffer(eq(0), geq(BranchableInputStream.DEFAULT_BUFFER_SIZE));
        
        impl.ensureBuffer(BranchableInputStream.DEFAULT_BUFFER_SIZE + 1);
        verify(impl).reallocateBuffer(eq(0), geq(BranchableInputStream.DEFAULT_BUFFER_SIZE * 2)); 

        impl.ensureBuffer(BranchableInputStream.DEFAULT_BUFFER_SIZE * 2 + 1);
        verify(impl).reallocateBuffer(eq(0), geq(BranchableInputStream.DEFAULT_BUFFER_SIZE * 4)); 
    }
}
