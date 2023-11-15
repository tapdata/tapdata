package io.tapdata.flow.engine.V2.node.hazelcast;

import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectInterceptor;

/**
 * @author samuel
 * @Description
 * @create 2023-11-14 19:09
 **/
public class AlwaysIntercept implements AspectInterceptor<MockDataFunctionAspect> {

	@Override
	public AspectInterceptResult intercept(MockDataFunctionAspect aspect) {
		AspectInterceptResult aspectInterceptResult = new AspectInterceptResult();
		aspectInterceptResult.intercepted(true);
		return aspectInterceptResult;
	}
}
