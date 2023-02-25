package io.tapdata.pdk.core.utils;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.pdk.apis.utils.OrderedIdGenerator;

@Implementation(OrderedIdGenerator.class)
public class OrderedIdGeneratorImpl implements OrderedIdGenerator {
	@Override
	public long nextId() {
		return IdUtil.nextId();
	}
}
