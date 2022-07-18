package com.tapdata.cache.hazelcast.serializer;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;

import javax.annotation.Nonnull;
import java.io.IOException;

public class HazelcastDataFlowCacheConfigSerializer implements StreamSerializer<DataFlowCacheConfig> {
	@Override
	public int getTypeId() {
		return 432153642;
	}

	@Override
	public void destroy() {
		StreamSerializer.super.destroy();
	}

	@Override
	public void write(@Nonnull ObjectDataOutput out, @Nonnull DataFlowCacheConfig object) throws IOException {
		out.writeString(JSONUtil.obj2Json(object));
	}

	@Nonnull
	@Override
	public DataFlowCacheConfig read(@Nonnull ObjectDataInput in) throws IOException {
		return JSONUtil.json2POJO(in.readString(), DataFlowCacheConfig.class);
	}
}
