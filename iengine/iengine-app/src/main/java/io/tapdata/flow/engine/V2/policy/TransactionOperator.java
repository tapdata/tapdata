package io.tapdata.flow.engine.V2.policy;

/**
 *
 * @author samuel
 * @Description
 * @create 2026-01-27 12:02
 **/
public interface TransactionOperator {
	void transactionBegin();
	void transactionCommit();
	void transactionRollback();
}
