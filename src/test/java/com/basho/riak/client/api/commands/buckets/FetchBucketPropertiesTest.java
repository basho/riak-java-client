package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.core.query.Namespace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FetchBucketPropertiesTest
{
    @Test
    public void equalsReturnsTrueForEqualNamespaces()
    {
        Namespace namespace1 = new Namespace("namespace");
        Namespace namespace2 = new Namespace("namespace");

        FetchBucketProperties fetchBucketProperties1 = new FetchBucketProperties.Builder(namespace1).build();
        FetchBucketProperties fetchBucketProperties2 = new FetchBucketProperties.Builder(namespace2).build();

        assertThat(fetchBucketProperties1, is(equalTo(fetchBucketProperties2)));
        assertThat(fetchBucketProperties2, is(equalTo(fetchBucketProperties1)));
    }

    @Test
    public void equalsReturnsFalseForDifferentNamespaces()
    {
        Namespace namespace1 = new Namespace("namespace1");
        Namespace namespace2 = new Namespace("namespace2");

        FetchBucketProperties fetchBucketProperties1 = new FetchBucketProperties.Builder(namespace1).build();
        FetchBucketProperties fetchBucketProperties2 = new FetchBucketProperties.Builder(namespace2).build();

        assertThat(fetchBucketProperties1, is(not(equalTo(fetchBucketProperties2))));
        assertThat(fetchBucketProperties2, is(not(equalTo(fetchBucketProperties1))));
    }
}
