package io.tapdata.flow.engine.V2.policy;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2024-07-09 11:00
 **/
public abstract class NodeWritePolicyService extends BaseWritePolicyService {
	protected Node<?> node;
	protected Map<String, Boolean> startTransactionMap;
	protected TransactionOperator transactionOperator;

	protected NodeWritePolicyService(TaskDto taskDto, Node<?> node) {
		super(taskDto);
		this.node = node;
	}

	@Override
	public void setStartTransactionMap(Map<String, Boolean> startTransactionMap) {
		this.startTransactionMap = startTransactionMap;
	}

	@Override
	public void setTransactionOperator(TransactionOperator transactionOperator) {
		this.transactionOperator = transactionOperator;
	}
}
