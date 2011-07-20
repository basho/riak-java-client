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
package com.basho.riak.client.http;

import static com.basho.riak.client.util.CharsetUtils.*;

import static org.junit.Assert.*;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.basho.riak.client.http.RiakClient;
import com.basho.riak.client.http.RiakLink;
import com.basho.riak.client.http.RiakObject;
import com.basho.riak.client.http.request.RequestMeta;
import com.basho.riak.client.http.response.FetchResponse;
import com.basho.riak.client.http.response.HttpResponse;
import com.basho.riak.client.http.response.StoreResponse;
import com.basho.riak.client.http.response.WalkResponse;
import com.basho.riak.client.http.util.Constants;

public class TestRiakObject {

    RiakObject impl;

    @Before public void setup() {
        impl = new RiakObject("b", "k");
    }

    @Test public void content_type_defaults_to_octet_stream() {
        assertEquals("application/octet-stream", impl.getContentType());
    }

    @Test public void links_never_null() {
        impl = new RiakObject("b", "k", null, null, null, null, null, null, null);
        assertNotNull(impl.getLinks());

        impl.setLinks((List<RiakLink>) null);
        assertNotNull(impl.getLinks());

        impl.copyData(new RiakObject(null, null));
        assertNotNull(impl.getLinks());
    }

    @Test public void usermeta_never_null() {
        impl = new RiakObject("b", "k", null, null, null, null, null, null, null);
        assertNotNull(impl.getUsermeta());

        impl.setUsermeta((Map<String, String>) null);
        assertNotNull(impl.getUsermeta());

        impl.copyData(new RiakObject(null, null));
        assertNotNull(impl.getUsermeta());
    }

    @Test public void copyData_does_deep_copy() {
        final String value = "value";
        final InputStream valueStream = mock(InputStream.class);
        final long valueStreamLength = 10;
        final String ctype = Constants.CTYPE_JSON_UTF8;
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        usermeta.put("testKey", "testValue");
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t");
        links.add(link);

        RiakObject copy = new RiakObject("b", "k2", utf8StringToBytes(value), ctype, links, usermeta, vclock, lastmod, vtag);
        copy.setValueStream(valueStream, valueStreamLength);
        impl.copyData(copy);

        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertEquals(value, impl.getValue());
        assertSame(valueStream, impl.getValueStream());
        assertEquals(valueStreamLength, impl.getValueStreamLength().longValue());
        assertEquals(ctype, impl.getContentType());

        // links and impl links are equal
        assertEquals(links.size(), impl.numLinks());
        for(RiakLink l : links) {
            assertTrue(impl.hasLink(l));
        }

        // user meta are equal
        assertEquals(usermeta.size(), impl.numUsermetaItems());

        for(Map.Entry<String, String> e : usermeta.entrySet()) {
            assertTrue(impl.hasUsermetaItem(e.getKey()));
            assertEquals(e.getValue(), impl.getUsermetaItem(e.getKey()));
        }

        assertEquals(vclock, impl.getVclock());
        assertEquals(lastmod, impl.getLastmod());
        assertEquals(vtag, impl.getVtag());

        assertNotSame(copy.getValueAsBytes(), impl.getValueAsBytes());

        // assert that the collections *and* contents of the collections are not the same
        // for the copy and the original object
        assertEquals(copy.numLinks(), impl.numLinks());
        for(RiakLink l : impl.iterableLinks()) {
            assertTrue(copy.hasLink(l));
            l.setKey("new Key");
            assertFalse(copy.hasLink(l));
            assertTrue(impl.hasLink(l));
        }

        assertEquals(copy.numUsermetaItems(), impl.numUsermetaItems());
        for(String key : impl.usermetaKeys()) {
            assertTrue(impl.hasUsermetaItem(key));
            assertTrue(copy.hasUsermetaItem(key));
            assertEquals(copy.getUsermetaItem(key), impl.getUsermetaItem(key));
        }

        impl.removeUsermetaItem("testKey");
        assertEquals(0, impl.numUsermetaItems());
        assertEquals(1, copy.numUsermetaItems());
    }

    @Test public void copyData_copies_null_data() {
        final String value = "value";
        final String ctype = Constants.CTYPE_JSON_UTF8;
        final List<RiakLink> links = new ArrayList<RiakLink>();
        final Map<String, String> usermeta = new HashMap<String, String>();
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";
        final RiakLink link = new RiakLink("b", "l", "t");
        links.add(link);

        impl = new RiakObject("b", "k", utf8StringToBytes(value), ctype, links, usermeta, vclock, lastmod, vtag);
        impl.copyData(new RiakObject(null, null));

        assertEquals("b", impl.getBucket());
        assertEquals("k", impl.getKey());
        assertNull(impl.getValue());
        assertEquals(0, impl.numLinks());
        assertEquals(0, impl.numUsermetaItems());
        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());
    }

    @Test public void updateMeta_nulls_out_meta_when_given_null_response() {
        final String vclock = "vclock";
        final String lastmod = "lastmod";
        final String vtag = "vtag";

        impl = new RiakObject("b", "k", null, null, null, null, vclock, lastmod, vtag);
        impl.updateMeta((StoreResponse) null);

        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());

        impl = new RiakObject("b", "k", null, null, null, null, vclock, lastmod, vtag);
        impl.updateMeta((FetchResponse) null);

        assertNull(impl.getVclock());
        assertNull(impl.getLastmod());
        assertNull(impl.getVtag());
    }

    @Test public void value_stream_is_separate_from_value() {
        final String value = "value";
        final byte[] isvalue = utf8StringToBytes("isbytes");
        final InputStream is = new ByteArrayInputStream(isvalue);

        impl.setValue(value);
        impl.setValueStream(is);

        assertEquals(value, impl.getValue());
        assertSame(is, impl.getValueStream());
    }

    @Test public void convenience_riak_client_methods_defer_to_riak_client() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        RequestMeta mockRequestMeta = mock(RequestMeta.class);
        StoreResponse mockStoreResponse = mock(StoreResponse.class);
        FetchResponse mockFetchResponse = mock(FetchResponse.class);
        HttpResponse mockHttpResponse = mock(HttpResponse.class);
        WalkResponse mockWalkResponse = mock(WalkResponse.class);
        
        when(mockRiakClient.store(any(RiakObject.class), any(RequestMeta.class))).thenReturn(mockStoreResponse);
        impl = new RiakObject(mockRiakClient, "b", "k");
        StoreResponse sr1 = impl.store();
        StoreResponse sr2 = impl.store(mockRequestMeta);
        
        verify(mockRiakClient).store(impl, null);        
        verify(mockRiakClient).store(impl, mockRequestMeta);
        assertSame(mockStoreResponse, sr1);
        assertSame(mockStoreResponse, sr2);

        when(mockRiakClient.fetchMeta(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        FetchResponse fr1 = impl.fetchMeta();
        FetchResponse fr2 = impl.fetchMeta(mockRequestMeta);
        
        verify(mockRiakClient).fetchMeta("b", "k", null);        
        verify(mockRiakClient).fetchMeta("b", "k", mockRequestMeta);
        assertSame(mockFetchResponse, fr1);
        assertSame(mockFetchResponse, fr2);

        when(mockRiakClient.fetch(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        fr1 = impl.fetch();
        fr2 = impl.fetch(mockRequestMeta);
        
        verify(mockRiakClient).fetch("b", "k", null);        
        verify(mockRiakClient).fetch("b", "k", mockRequestMeta);        
        assertSame(mockFetchResponse, fr1);
        assertSame(mockFetchResponse, fr2);

        when(mockRiakClient.delete(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockHttpResponse);
        HttpResponse hr1 = impl.delete();
        HttpResponse hr2 = impl.delete(mockRequestMeta);
        
        verify(mockRiakClient).delete("b", "k", null);        
        verify(mockRiakClient).delete("b", "k", mockRequestMeta);
        assertSame(mockHttpResponse, hr1);
        assertSame(mockHttpResponse, hr2);

        when(mockRiakClient.walk(anyString(), anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockWalkResponse);
        WalkResponse wr1 = impl.walk().run();
        WalkResponse wr2 = impl.walk().run(mockRequestMeta);
        
        verify(mockRiakClient).walk("b", "k", "_,_,_/", null);        
        verify(mockRiakClient).walk("b", "k", "_,_,_/", mockRequestMeta);
        assertSame(mockWalkResponse, wr1);
        assertSame(mockWalkResponse, wr2);
    }
    
    @Test public void store_updates_meta_on_success() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        StoreResponse mockStoreResponse = mock(StoreResponse.class);
        
        when(mockRiakClient.store(any(RiakObject.class), any(RequestMeta.class))).thenReturn(mockStoreResponse);
        when(mockStoreResponse.isSuccess()).thenReturn(true);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.store();
        
        verify(impl).updateMeta(mockStoreResponse);
    }

    @Test public void store_does_not_modify_meta_on_failure() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        StoreResponse mockStoreResponse = mock(StoreResponse.class);
        
        when(mockRiakClient.store(any(RiakObject.class), any(RequestMeta.class))).thenReturn(mockStoreResponse);
        when(mockStoreResponse.isSuccess()).thenReturn(false);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.store();
        
        verify(impl, never()).updateMeta(any(StoreResponse.class));
        verify(impl, never()).copyData(any(RiakObject.class));
    }

    @Test public void fetchMeta_updates_meta_on_success() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        FetchResponse mockFetchResponse = mock(FetchResponse.class);
        
        when(mockRiakClient.fetchMeta(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        when(mockFetchResponse.isSuccess()).thenReturn(true);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.fetchMeta();
        
        verify(impl).updateMeta(mockFetchResponse);
    }

    @Test public void fetchMeta_does_not_modify_meta_on_failure() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        FetchResponse mockFetchResponse = mock(FetchResponse.class);
        
        when(mockRiakClient.fetchMeta(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        when(mockFetchResponse.isSuccess()).thenReturn(false);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.fetchMeta();
        
        verify(impl, never()).updateMeta(any(FetchResponse.class));
        verify(impl, never()).copyData(any(RiakObject.class));
    }

    @Test public void fetch_shallow_copies_object_on_success() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        FetchResponse mockFetchResponse = mock(FetchResponse.class);
        RiakObject mockRiakObject = mock(RiakObject.class);
        when(mockRiakClient.fetch(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        when(mockFetchResponse.isSuccess()).thenReturn(true);
        when(mockFetchResponse.getObject()).thenReturn(mockRiakObject);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.fetch();
        
        verify(impl).shallowCopy(mockRiakObject);
        verify(mockFetchResponse).setObject(impl);
    }

    @Test public void fetch_does_not_update_data_on_failure() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        FetchResponse mockFetchResponse = mock(FetchResponse.class);
        
        when(mockRiakClient.fetch(anyString(), anyString(), any(RequestMeta.class))).thenReturn(mockFetchResponse);
        when(mockFetchResponse.isSuccess()).thenReturn(false);
        when(mockFetchResponse.getObject()).thenReturn(null);
        impl = spy(new RiakObject(mockRiakClient, "b", "k"));
        impl.fetch();
        
        verify(impl, never()).updateMeta(any(FetchResponse.class));
        verify(impl, never()).shallowCopy(any(RiakObject.class));
        verify(impl, never()).copyData(any(RiakObject.class));
    }
    
    @Test public void walk_returns_one_step_correctly() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        
        impl = new RiakObject(mockRiakClient, "b", "k");

        impl.walk().run();
        verify(mockRiakClient).walk("b", "k", "_,_,_/", null);

        impl.walk(false).run();
        verify(mockRiakClient).walk("b", "k", "_,_,0/", null);

        impl.walk("bucket").run();
        verify(mockRiakClient).walk("b", "k", "bucket,_,_/", null);

        impl.walk("bucket", false).run();
        verify(mockRiakClient).walk("b", "k", "bucket,_,0/", null);

        impl.walk("bucket", "tag").run();
        verify(mockRiakClient).walk("b", "k", "bucket,tag,_/", null);

        impl.walk("bucket", "tag", false).run();
        verify(mockRiakClient).walk("b", "k", "bucket,tag,0/", null);
    }

    @Test public void walk_returns_multiple_step_correctly() {
        RiakClient mockRiakClient = mock(RiakClient.class);
        impl = new RiakObject(mockRiakClient, "b", "k");

        impl.walk().walk("bucket").walk("bucket", "tag", false).run();
        
        verify(mockRiakClient).walk("b", "k", "_,_,_/bucket,_,_/bucket,tag,0/", null);    
    }

    // The following could be combined as "convenience_riak_client_methods_throw_if_no_associated_riak_client"
    @Test(expected=IllegalStateException.class) public void store_throws_if_no_associated_riak_client() {
        impl.store();
    }
    @Test(expected=IllegalStateException.class) public void fetchMeta_throws_if_no_associated_riak_client() {
        impl.fetchMeta();
    }
    @Test(expected=IllegalStateException.class) public void fetch_throws_if_no_associated_riak_client() {
        impl.fetch();
    }
    @Test(expected=IllegalStateException.class) public void delete_throws_if_no_associated_riak_client() {
        impl.delete();
    }
    @Test(expected=IllegalStateException.class) public void walk_throws_if_no_associated_riak_client() {
        impl.walk().run();
    }

    @SuppressWarnings("unchecked") @Test public void write_to_http_method_gives_value_stream_priority_over_value() throws URISyntaxException {
        final String value = "value";
        final byte[] isvalue = utf8StringToBytes("isbytes");
        final InputStream is = new ByteArrayInputStream(isvalue);
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("http://host:9999/path/to/object"));
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((HttpEntity) invocation.getArguments()[0]).writeTo(os);
                return null;
            }
        }).when(mockHttpMethod).setEntity(any(HttpEntity.class));

        impl.setValue(value);
        impl.setValueStream(is);
        impl.writeToHttpMethod(mockHttpMethod);

        assertArrayEquals(os.toByteArray(), isvalue);
    }

    @Test public void write_to_http_method_always_sets_entity_even_if_value_is_null() {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        impl.setValue((String) null);
        impl.setValueStream(null);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).setEntity((HttpEntity) notNull());
    }

    @Test public void write_to_http_method_sets_link_header() throws URISyntaxException {
        final RiakLink link = new RiakLink("b", "l", "t");
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/riak/b/k"));

        impl.addLink(link);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).addHeader(eq(Constants.HDR_LINK), contains("</riak/b/l>; riaktag=\"t\""));
    }

    @Test public void write_to_http_method_doesnt_sets_link_header_if_no_links() {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod, never()).addHeader(eq(Constants.HDR_LINK), anyString());
    }

    @Test public void write_to_http_method_sets_user_meta_headers() {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        impl.addUsermetaItem("k", "v");
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).addHeader(Constants.HDR_USERMETA_REQ_PREFIX + "k", "v");
    }

    @Test public void write_to_http_method_doesnt_sets_user_meta_headers_if_no_usermeta() {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod, never()).addHeader(contains(Constants.HDR_USERMETA_REQ_PREFIX), anyString());
    }

    @Test public void write_to_http_method_sets_vclock() {
        final String vclock = "vclock";
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        impl = new RiakObject("b", "k", null, null, null, null, vclock, null, null);
        impl.writeToHttpMethod(mockHttpMethod);

        verify(mockHttpMethod).addHeader(Constants.HDR_VCLOCK, vclock);
    }

    @Test public void get_base_path_finds_empty_base_path() throws URISyntaxException {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(null);
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getURI()).thenReturn(new URI(""));
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getURI()).thenReturn(new URI("/"));
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getURI()).thenReturn(new URI("/b"));
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));

        when(mockHttpMethod.getURI()).thenReturn(new URI("/b/k"));
        assertEquals("", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_finds_one_element_base_path() throws URISyntaxException {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/riak/b/k"));
        assertEquals("/riak", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_finds_multiple_element_base_path() throws URISyntaxException {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/path/to/riak/b/k"));
        assertEquals("/path/to/riak", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    @Test public void get_base_path_handles_trailing_slash() throws URISyntaxException {
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/riak/b/k/"));
        assertEquals("/riak", impl.getBasePathFromHttpMethod(mockHttpMethod));
    }

    /*
     * Encapsulated links
     */

    @Test public void linkEncapsulationIsAsGoodAsDirectAccess() {
        final List<RiakLink> links = Arrays.asList(new RiakLink("b", "l", "t"), new RiakLink("b", "e", "c"),
                                                   new RiakLink("g", "c", "s"), new RiakLink("q", "p", "c"));

        final RiakObject riakObject = new RiakObject("b", "k", utf8StringToBytes("v"), "", links, (Map<String, String>) null, "", "", "");

        assertEquals(links.size(), riakObject.numLinks());
        assertTrue(riakObject.hasLinks());

        for(RiakLink link : riakObject.iterableLinks()) {
            assertTrue(links.contains(link));
        }

        //updates on iterator fail
        try {
            riakObject.iterableLinks().iterator().remove();
            fail("Expected UnsupportedOperationException");
        } catch(UnsupportedOperationException e) {
            //NO-OP
        }

        //updates on collection succeed
        final RiakLink addedLink = new RiakLink("s", "e", "c");
        riakObject.getLinks().add(addedLink);
        assertTrue(linkPresent(riakObject, addedLink));

        riakObject.getLinks().remove(addedLink);
        assertFalse(linkPresent(riakObject, addedLink));

        //updates from encapsulation API succeed
        riakObject.addLink(addedLink);
        assertTrue(linkPresent(riakObject, addedLink));

        riakObject.removeLink(addedLink);
        assertFalse(linkPresent(riakObject, addedLink));

        riakObject.addLink(addedLink);
        assertTrue(riakObject.hasLink(addedLink));

        riakObject.removeLink(addedLink);
        assertFalse(riakObject.hasLink(addedLink));
    }

    /**
     * Checks if the riakObject's collection of RiakLinks contains the passed RiakLink.
     * @param riakObject the RiakObject to test
     * @param link the RiakLink to test
     * @return true if the RiakObject's collection of links contains the RiakLink.
     */
    private boolean linkPresent(final RiakObject riakObject, final RiakLink link) {
        boolean linkPresent = false;

        for (RiakLink l : riakObject.iterableLinks()) {
            if (l.equals(link)) {
                linkPresent = true;
            }
        }
        return linkPresent;
    }

    /*
     * Encapsulated user meta
     */
    @Test public void userMetaEncapsulationIsAsGoodAsDirectAccess() {
        final Map<String, String> userMeta = new HashMap<String, String>();
        userMeta.put("acl", "admin");
        userMeta.put("my-meta", "my-value");

        final RiakObject riakObject = new RiakObject("b", "k", utf8StringToBytes("v"), "", null, userMeta, "", "", "");

        assertTrue(riakObject.hasUsermeta());
        assertTrue(riakObject.hasUsermetaItem("acl"));
        assertTrue(riakObject.hasUsermetaItem("my-meta"));

        userMeta.clear();

        assertTrue(riakObject.hasUsermetaItem("acl"));
        assertTrue(riakObject.hasUsermetaItem("my-meta"));

        riakObject.addUsermetaItem("my-meta2", "my-value2");

        assertTrue(riakObject.hasUsermetaItem("my-meta2"));
        assertEquals("my-value2", riakObject.getUsermetaItem("my-meta2"));

        riakObject.removeUsermetaItem("acl");

        assertFalse(riakObject.hasUsermetaItem("acl"));

        final Map<String, String> leakedUserMeta = riakObject.getUsermeta();

        assertEquals("my-value2", leakedUserMeta.get("my-meta2"));
        assertEquals("my-value", leakedUserMeta.get("my-meta"));

        // still writes through
        leakedUserMeta.put("my-meta3", "my-value3");
        assertTrue(riakObject.hasUsermetaItem("my-meta3"));
        assertEquals("my-value3", riakObject.getUsermetaItem("my-meta3"));
    }

    @Test public void modifyLinksAndWriteToMethodConcurrently() throws InterruptedException, URISyntaxException {
        final RiakObject riakObject = new RiakObject("b", "k", utf8StringToBytes("v"));
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/riak/b/k"));

        final int cnt = 20;
        Thread[] threads = new Thread[cnt];

        for (int i = 0; i < cnt; i++) {
            threads[i] = new Thread(new Runnable() {

                public void run() {
                    String bucket = UUID.randomUUID().toString();
                    String key = UUID.randomUUID().toString();
                    String tag = UUID.randomUUID().toString();
                    int cnt = 0;
                    while (true) {
                        riakObject.addLink(new RiakLink(bucket + cnt, key + cnt, tag + cnt));
                        cnt++;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        Thread.sleep(500);

        riakObject.writeToHttpMethod(mockHttpMethod);
    }

    @Test public void modifyUserMetaAndWriteToMethodConcurrently() throws InterruptedException, URISyntaxException {
        final RiakObject riakObject = new RiakObject("b", "k", utf8StringToBytes("v"));
        final HttpEntityEnclosingRequestBase mockHttpMethod = mock(HttpEntityEnclosingRequestBase.class);

        when(mockHttpMethod.getURI()).thenReturn(new URI("/riak/b/k"));

        final int cnt = 20;
        Thread[] threads = new Thread[cnt];

        for (int i = 0; i < cnt; i++) {
            threads[i] = new Thread(new Runnable() {

                public void run() {
                    String key = UUID.randomUUID().toString();
                    String tag = UUID.randomUUID().toString();
                    int cnt = 0;
                    while (true) {
                        riakObject.addUsermetaItem(key + cnt, tag + cnt);
                        cnt++;
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }

                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        Thread.sleep(500);

        riakObject.writeToHttpMethod(mockHttpMethod);
    }

    @Test public void valueByteArraySafelyEncapsulated() {
        final byte[] value = utf8StringToBytes("vvvv");
        final RiakObject riakObject = new RiakObject("b", "k", value);

        assertArrayEquals(utf8StringToBytes("vvvv"), riakObject.getValueAsBytes());
        value[0] = 'b';
        assertArrayEquals(utf8StringToBytes("vvvv"), riakObject.getValueAsBytes());

        byte[] roInternalValue = riakObject.getValueAsBytes();
        roInternalValue[0] = 'z';

        assertArrayEquals(utf8StringToBytes("vvvv"), riakObject.getValueAsBytes());
    }
}
