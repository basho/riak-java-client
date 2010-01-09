package com.basho.riak.client.raw;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.RiakLink;
import com.basho.riak.client.util.Constants;

public class TestRawObject {

    RawObject impl;

    @Before public void setup() {
        impl = new RawObject("b", "k");
    }

    @Test public void content_type_defaults_to_octet_stream() {
        assertEquals("application/octet-stream", impl.getContentType());
    }

    @Test public void links_never_null() {
        impl = new RawObject("b", "k", null, null, null, null, null, null, null);
        assertNotNull(impl.getLinks());

        impl.setLinks((List<RiakLink>) null);
        assertNotNull(impl.getLinks());

        impl.copyData(new RawObject(null, null));
        assertNotNull(impl.getLinks());
    }

    @Test public void usermeta_never_null() {
        impl = new RawObject("b", "k", null, null, null, null, null, null, null);
        assertNotNull(impl.getUsermeta());

        impl.setUsermeta((Map<String, String>) null);
        assertNotNull(impl.getUsermeta());

        impl.copyData(new RawObject(null, null));
        assertNotNull(impl.getUsermeta());
    }

    @Test public void copyData_does_deep_copy() {
        final String value = "value";
        final String ctype = "ctype";
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t");
        links.add(link);

        impl.copyData(new RawObject("b", "k2", value, ctype, links, usermeta, vclock, lastmod, vtag));

        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertEquals(value, impl.getValue());
        assertEquals(ctype, impl.getContentType());
        assertEquals(links, impl.getLinks());
        assertEquals(usermeta, impl.getUsermeta());
        assertEquals(vclock, impl.getVclock());
        assertEquals(lastmod, impl.getLastmod());
        assertEquals(vtag, impl.getVtag());

        assertNotSame(links, impl.getLinks());
        assertNotSame(link, impl.getLinks().get(0));
        assertNotSame(usermeta, impl.getUsermeta());
    }

    @Test public void copyData_copies_null_data() {
        final String value = "value";
        final String ctype = "ctype";
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t");
        links.add(link);

        impl = new RawObject("b", "k", value, ctype, links, usermeta, vclock, lastmod, vtag);
        impl.copyData(new RawObject(null, null));

        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertNull(impl.getValue());
        assertEquals(0, impl.getLinks().size());
        assertEquals(0, impl.getUsermeta().size());
        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());
    }

    @Test public void updateMeta_nulls_out_meta_when_given_null_response() {
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";

        impl = new RawObject("b", "k", null, null, null, null, vclock, lastmod, vtag);
        impl.updateMeta(null);

        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());
    }

    @Test public void value_stream_is_separate_from_value() {
        final String value = "value";
        final byte[] isvalue = "isbytes".getBytes();
        final InputStream is = new ByteArrayInputStream(isvalue);

        impl.setValue(value);
        impl.setValueStream(is);

        assertEquals(value, impl.getValue());
        assertSame(is, impl.getValueStream());
    }

    @SuppressWarnings("unchecked") @Test public void write_to_http_method_gives_value_stream_priority_over_value() {
        final String value = "value";
        final byte[] isvalue = "isbytes".getBytes();
        final InputStream is = new ByteArrayInputStream(isvalue);
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn("/path/to/object");
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((RequestEntity) invocation.getArguments()[0]).writeRequest(os);
                return null;
            }
        }).when(mockHttpMethod).setRequestEntity(any(RequestEntity.class));

        impl.setValue(value);
        impl.setValueStream(is);
        impl.writeToHttpMethod(mockHttpMethod);

        assertArrayEquals(os.toByteArray(), isvalue);
    }

    @Test public void write_to_http_method_always_sets_entity_even_if_value_is_null() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        impl.setValue((String) null);
        impl.setValueStream(null);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).setRequestEntity((RequestEntity) notNull());
    }

    @Test public void write_to_http_method_sets_link_header() {
        final RiakLink link = new RiakLink("b", "l", "t");
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn("/raw/b/k");

        impl.getLinks().add(link);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).setRequestHeader(eq(Constants.HDR_LINK), contains("</raw/b/l>; riaktag=\"t\""));
    }

    @Test public void write_to_http_method_doesnt_sets_link_header_if_no_links() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod, never()).setRequestHeader(eq(Constants.HDR_LINK), anyString());
    }

    @Test public void write_to_http_method_sets_user_meta_headers() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        impl.getUsermeta().put("k", "v");
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).setRequestHeader(Constants.HDR_USERMETA_PREFIX + "k", "v");
    }

    @Test public void write_to_http_method_doesnt_sets_user_meta_headers_if_no_usermeta() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod, never()).setRequestHeader(contains(Constants.HDR_USERMETA_PREFIX), anyString());
    }

    @Test public void write_to_http_method_sets_vclock() {
        final String vclock = "vclock";
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        impl = new RawObject("b", "k", null, null, null, null, vclock, null, null);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).setRequestHeader(Constants.HDR_VCLOCK, vclock);
    }

    @Test public void get_base_path_finds_empty_base_path() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn(null);
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getPath()).thenReturn("");
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getPath()).thenReturn("/");
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getPath()).thenReturn("/b");
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getPath()).thenReturn("/b/k");
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_finds_one_element_base_path() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn("/raw/b/k");
        assertEquals("/raw", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_finds_multiple_element_base_path() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn("/path/to/raw/b/k");
        assertEquals("/path/to/raw", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_handles_trailing_slash() {
        final EntityEnclosingMethod mockHttpMethod = mock(EntityEnclosingMethod.class);

        when(mockHttpMethod.getPath()).thenReturn("/raw/b/k/");
        assertEquals("/raw", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }
}
