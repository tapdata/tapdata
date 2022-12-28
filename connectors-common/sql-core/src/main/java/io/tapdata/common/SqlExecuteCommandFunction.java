package io.tapdata.common;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.error.NotSupportedException;
import io.tapdata.pdk.apis.functions.TapSupplier;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SqlExecuteCommandFunction {

  private static DefaultSqlExecutor sqlExecutor = new DefaultSqlExecutor();

  public static void executeCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Consumer<ExecuteResult> executeResultConsumer) {

    ExecuteResult<?> executeResult;
    try {
      Map<String, Object> params = tapExecuteCommand.getParams();
      String command = tapExecuteCommand.getCommand();
      switch (command) {
        case "execute":
        case "executeQuery":
          String sql = (String) params.get("sql");
          executeResult = sqlExecutor.execute(sql, connectionSupplier);
          break;
        case "call":
          String funcName = (String) params.get("funcName");
          List<Map<String, Object>> callParams = (List<Map<String, Object>>) params.get("params");
          executeResult = sqlExecutor.call(funcName, callParams, connectionSupplier);
          break;
        default:
          executeResult = new ExecuteResult<>().error(new NotSupportedException(command));
      }
    } catch (Exception e) {
      executeResult = new ExecuteResult<>().error(e);
    }
    executeResultConsumer.accept(executeResult);
  }

  public static void setSqlExecutor(DefaultSqlExecutor sqlExecutor) {
    SqlExecuteCommandFunction.sqlExecutor = sqlExecutor;
  }
}
