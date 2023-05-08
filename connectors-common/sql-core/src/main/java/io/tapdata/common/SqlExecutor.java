package io.tapdata.common;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.functions.TapSupplier;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface SqlExecutor {

	void execute(String sql, TapSupplier<Connection> connectionSupplier, Consumer<Object> consumer, Supplier<Boolean> aliveSupplier, int batchSize) throws Throwable;

	ExecuteResult<?> execute(String sql, TapSupplier<Connection> connectionSupplier);

	ExecuteResult<?> call(String funcName, List<Map<String, Object>> params, TapSupplier<Connection> connectionSupplier);
}
