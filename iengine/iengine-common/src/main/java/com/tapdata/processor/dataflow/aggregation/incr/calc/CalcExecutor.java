package com.tapdata.processor.dataflow.aggregation.incr.calc;

@FunctionalInterface
public interface CalcExecutor<R> {

	R exec(Calculator calculator, Number p1, Number p2);

}
