package com.tapdata.cache.hazelcast.serializer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.tapdata.cache.hazelcast.HazelcastCacheStats;

import javax.annotation.Nonnull;
import java.io.IOException;

public class HazelcastCacheStatsSerializer implements StreamSerializer<HazelcastCacheStats> {
	@Override
	public void write(@Nonnull ObjectDataOutput out, @Nonnull HazelcastCacheStats object) throws IOException {
		out.writeLongArray(object.toLongArray());
	}

	@Nonnull
	@Override
	public HazelcastCacheStats read(@Nonnull ObjectDataInput in) throws IOException {
		long[] array = in.readLongArray();
		return new HazelcastCacheStats(
				array[0],
				array[1],
				array[2],
				array[3]);
	}

	@Override
	public int getTypeId() {
		return 1234565769;
	}

	@Override
	public void destroy() {
		StreamSerializer.super.destroy();
	}
}
