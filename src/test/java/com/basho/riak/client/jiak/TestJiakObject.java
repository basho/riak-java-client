package com.basho.riak.client.jiak;

import org.junit.Test;

public class TestJiakObject {

    @Test public void constructor_from_json_parses_all_fields() {
        
    }
    
    @Test public void content_type_is_json() {
        
    }
    
    @Test public void links_never_null() {
        
    }
    
    @Test public void copyData_does_deep_copy() {
        
    }

    @Test(expected=IllegalArgumentException.class) public void set_value_throws_on_illegal_json() {
        
    }
    
    @Test public void get_usermeta_as_json_returns_all_usermeta() {
        
    }
    
    @Test public void get_links_as_json_returns_all_links() {
        
    }
    
    @Test public void write_to_http_method_delegates_to_to_json_object() {
        
    }
    
    @Test public void to_json_object_adds_links() {
    }
    
    @Test public void to_json_object_adds_empty_links_array_if_none() {
    }
    
    @Test public void to_json_object_adds_usermeta() {
    }

    @Test public void to_json_object_adds_usermeta_even_if_no_value() {
    }

    @Test public void to_json_object_overwrites_usermeta_in_value() {
    }

    @Test public void to_json_object_doesnt_adds_usermeta_if_none() {
    }

    @Test public void to_json_object_always_adds_bucket_key_value_and_links_even_if_null() {
    }

    @Test public void to_json_object_sets_vclock_lastmod_and_vtag_when_not_null() {
    }

    @Test public void usermeta_never_null() {
        
    }
    
}
