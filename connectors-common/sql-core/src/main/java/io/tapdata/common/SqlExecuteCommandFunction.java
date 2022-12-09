package io.tapdata.common;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.TapSupplier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;

public class SqlExecuteCommandFunction {

  public static void executeCommand(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, TapSupplier<Connection> connectionSupplier, Consumer<ExecuteResult> executeResultConsumer) {

    ExecuteResult executeResult = new ExecuteResult();
    try (Connection connection = connectionSupplier.get();
         Statement sqlStatement = connection.createStatement()) {
      boolean isQuery = sqlStatement.execute(tapExecuteCommand.getCommand());
      if (isQuery) {
        try (ResultSet resultSet = sqlStatement.getResultSet()) {
          List<DataMap> dataMaps = DbKit.getDataFromResultSet(resultSet);
          executeResult.setResults(dataMaps);
        }
      } else {
        executeResult.setModifiedCount(sqlStatement.getUpdateCount());
      }
      connection.commit();

    } catch (Throwable e) {
      executeResult.setError(e);
    }
    executeResultConsumer.accept(executeResult);
  }
}
