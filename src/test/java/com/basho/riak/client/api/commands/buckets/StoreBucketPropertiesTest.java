package com.basho.riak.client.api.commands.buckets;

import com.basho.riak.client.core.query.Namespace;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class StoreBucketPropertiesTest
{
    @Test
    public void equalsReturnsTrueForEqualBucketProperties()
    {
        StoreBucketProperties storeBucketProperties1 =
                new StoreBucketProperties.Builder(new Namespace("namespace"))
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
        StoreBucketProperties storeBucketProperties2 =
                new StoreBucketProperties.Builder(new Namespace("namespace"))
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

        assertThat(storeBucketProperties1, is(equalTo(storeBucketProperties2)));
        assertThat(storeBucketProperties2, is(equalTo(storeBucketProperties1)));
    }

    @Test
    public void equalsReturnsFalseForDifferentBucketProperties()
    {
        StoreBucketProperties storeBucketProperties1 =
                new StoreBucketProperties.Builder(new Namespace("namespace1"))
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

        StoreBucketProperties storeBucketProperties2 =
                new StoreBucketProperties.Builder(new Namespace("namespace2"))
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

        assertThat(storeBucketProperties1, is(not(equalTo(storeBucketProperties2))));
        assertThat(storeBucketProperties2, is(not(equalTo(storeBucketProperties1))));
    }
}
