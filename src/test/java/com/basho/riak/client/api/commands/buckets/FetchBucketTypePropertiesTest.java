package com.basho.riak.client.api.commands.buckets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Luke Bakken <lbakken@basho.com>
 */
@RunWith(MockitoJUnitRunner.class)
public class FetchBucketTypePropertiesTest
{
    @Test
    public void equalsReturnsTrueForEqualBucketType()
    {
        FetchBucketTypeProperties fp1 = new FetchBucketTypeProperties.Builder("bt").build();
        FetchBucketTypeProperties fp2 = new FetchBucketTypeProperties.Builder("bt").build();

        assertThat(fp1, is(equalTo(fp2)));
        assertThat(fp2, is(equalTo(fp1)));
    }

    @Test
    public void equalsReturnsFalseForDifferentBucketType()
    {
        FetchBucketTypeProperties fp1 = new FetchBucketTypeProperties.Builder("bt1").build();
        FetchBucketTypeProperties fp2 = new FetchBucketTypeProperties.Builder("bt2").build();

        assertThat(fp1, is(not(equalTo(fp2))));
        assertThat(fp2, is(not(equalTo(fp1))));
    }
}
