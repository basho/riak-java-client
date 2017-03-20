package com.basho.riak.client.api.commands.buckets;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Luke Bakken <lbakken@basho.com>
 */
public class StoreBucketTypePropertiesTest
{
    @Test
    public void equalsReturnsTrueForEqualBucketProperties()
    {
        final String bt = "bucketTypeName";
        StoreBucketTypeProperties s1 =
                new StoreBucketTypeProperties.Builder(bt)
                        .withBackend("backend")
                        .withNotFoundOk(true)
                        .withHllPrecision(15)
                        .withNVal(3)
                        .withPr(1)
                        .withPw(2)
                        .withR(1)
                        .withRw(1)
                        .withW(1)
                        .withDw(3)
                        .withBasicQuorum(false)
                        .withAllowMulti(true)
                        .withLastWriteWins(true)
                        .withBigVClock(42L)
                        .withOldVClock(41L)
                        .withSmallVClock(5L)
                        .withYoungVClock(19L)
                        .withLegacyRiakSearchEnabled(false)
                        .withSearchIndex("index")
                        .build();
        StoreBucketTypeProperties s2 =
                new StoreBucketTypeProperties.Builder(bt)
                        .withBackend("backend")
                        .withNotFoundOk(true)
                        .withHllPrecision(15)
                        .withNVal(3)
                        .withPr(1)
                        .withPw(2)
                        .withR(1)
                        .withRw(1)
                        .withW(1)
                        .withDw(3)
                        .withBasicQuorum(false)
                        .withAllowMulti(true)
                        .withLastWriteWins(true)
                        .withBigVClock(42L)
                        .withOldVClock(41L)
                        .withSmallVClock(5L)
                        .withYoungVClock(19L)
                        .withLegacyRiakSearchEnabled(false)
                        .withSearchIndex("index")
                        .build();

        assertThat(s1, is(equalTo(s2)));
        assertThat(s2, is(equalTo(s1)));
    }

    @Test
    public void equalsReturnsFalseForDifferentBucketProperties()
    {
        StoreBucketTypeProperties s1 =
                new StoreBucketTypeProperties.Builder("bt1")
                        .withBackend("backend1")
                        .withNotFoundOk(true)
                        .withHllPrecision(14)
                        .withNVal(3)
                        .withPr(1)
                        .withPw(2)
                        .withR(1)
                        .withRw(1)
                        .withW(1)
                        .withDw(3)
                        .withBasicQuorum(false)
                        .withAllowMulti(true)
                        .withLastWriteWins(true)
                        .withBigVClock(42L)
                        .withOldVClock(41L)
                        .withSmallVClock(5L)
                        .withYoungVClock(19L)
                        .withLegacyRiakSearchEnabled(false)
                        .withSearchIndex("index1")
                        .build();

        StoreBucketTypeProperties s2=
                new StoreBucketTypeProperties.Builder("bt2")
                        .withBackend("backend2")
                        .withNotFoundOk(false)
                        .withHllPrecision(16)
                        .withNVal(5)
                        .withPr(2)
                        .withPw(4)
                        .withR(2)
                        .withRw(2)
                        .withW(2)
                        .withDw(4)
                        .withBasicQuorum(true)
                        .withAllowMulti(false)
                        .withLastWriteWins(false)
                        .withBigVClock(43L)
                        .withOldVClock(42L)
                        .withSmallVClock(4L)
                        .withYoungVClock(190L)
                        .withLegacyRiakSearchEnabled(true)
                        .withSearchIndex("index2")
                        .build();

        assertThat(s1, is(not(equalTo(s2))));
        assertThat(s2, is(not(equalTo(s1))));
    }
}
