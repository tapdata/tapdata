package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.function.ThrowableFunction;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 11:00
 **/
public interface WritePolicyService {
	void writeRecordWithPolicyControl(String tableId, List<TapRecordEvent> tapRecordEvents, ThrowableFunction<Void, List<TapRecordEvent>, Throwable> writePolicyRunner) throws Throwable;

	default void setStartTransactionMap(Map<String, Boolean> startTransactionMap) {
	}

	default void setTransactionOperator(TransactionOperator transactionOperator) {
	}
}
