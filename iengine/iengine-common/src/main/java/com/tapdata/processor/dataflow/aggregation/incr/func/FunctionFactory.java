package com.tapdata.processor.dataflow.aggregation.incr.func;

import com.tapdata.entity.dataflow.Aggregation;
import com.tapdata.processor.dataflow.aggregation.incr.cache.BucketCache;
import com.tapdata.processor.dataflow.aggregation.incr.func.impl.AvgFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.impl.CountFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.impl.MaxFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.impl.MinFunction;
import com.tapdata.processor.dataflow.aggregation.incr.func.impl.SumFunction;
import com.tapdata.processor.dataflow.aggregation.incr.service.model.BucketValue;

import java.util.Optional;

public class FunctionFactory {

	private static final FunctionFactory FACTORY = new FunctionFactory();

	private FunctionFactory() {
	}

	public static FunctionFactory getInstance() {
		return FACTORY;
	}

	public AggrFunction create(BucketCache<FuncCacheKey, BucketValue> bucketCache, Aggregation aggregation) throws Throwable {
		final Func funcName = Optional.of(Func.valueOf(aggregation.getAggFunction())).orElseThrow(() -> new IllegalArgumentException(String.format("unknown aggregation function: %s", aggregation.getName())));
		switch (funcName) {
			case SUM:
				return new SumFunction(bucketCache, aggregation);
			case COUNT:
				return new CountFunction(bucketCache, aggregation);
			case MAX:
				return new MaxFunction(bucketCache, aggregation);
			case MIN:
				return new MinFunction(bucketCache, aggregation);
			case AVG:
				return new AvgFunction(bucketCache, aggregation);
			default:
				throw new IllegalArgumentException(String.format("supported aggregation function: %s", aggregation.getName()));
		}
	}

}
