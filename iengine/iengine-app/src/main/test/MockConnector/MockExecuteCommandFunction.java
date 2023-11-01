package MockConnector;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MockExecuteCommandFunction implements ExecuteCommandFunction {


    @Override
    public void execute(TapConnectorContext tapConnectorContext, TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) throws Throwable {
            if(tapConnectorContext.getSpecification().getId().equals("mysql")){
                handleMysqlExecuteCommand(tapExecuteCommand,consumer);
            }
    }


    public void handleMysqlExecuteCommand(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        if (tapExecuteCommand.getCommand().contains("normal")) {
            handleMysqlNormal(tapExecuteCommand, consumer);
        } else if (tapExecuteCommand.getCommand().contains("normal-long")) {
            handleMysqlNormalLong(tapExecuteCommand, consumer);
        } else if (tapExecuteCommand.getCommand().contains("resultNull")) {
            handleMysqlResultNull(tapExecuteCommand, consumer);
        } else if(tapExecuteCommand.getCommand().contains("exception")){
            handleMysqlException(tapExecuteCommand, consumer);
        }else if (tapExecuteCommand.getCommand().contains("listNormal")) {
            handleMysqlNormalList(tapExecuteCommand, consumer);
        } else if (tapExecuteCommand.getCommand().contains("listNull")) {
            handleMysqlListNull(tapExecuteCommand, consumer);
        }

    }

    public void handleMysqlNormalLong(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        ExecuteResult executeResult = new ExecuteResult<Long>().result(100L);
        consumer.accept(executeResult);
    }

    public void handleMysqlNormal(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("COUNT(1)", 100);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        ExecuteResult executeResult = new ExecuteResult<List<Map<String, Object>>>().result(list);
        consumer.accept(executeResult);
    }

    public void handleMysqlResultNull(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
            ExecuteResult executeResult = new ExecuteResult<List<Map<String, Object>>>().result(null);
            consumer.accept(executeResult);

    }


    public void handleMysqlException(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        SQLException sqlException = new SQLSyntaxErrorException("Table 'mydb.test1' doesn't exist",
                "42S02", 1146);
        consumer.accept(new ExecuteResult<>().error(sqlException));

    }

    public void handleMysqlNormalList(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", "100");
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        ExecuteResult executeResult = new ExecuteResult<List<Map<String, Object>>>().result(list);
        consumer.accept(executeResult);
    }

    public void handleMysqlListNull(TapExecuteCommand tapExecuteCommand, Consumer<ExecuteResult> consumer) {
        List<Map<String, Object>> list = new ArrayList<>();
        ExecuteResult executeResult = new ExecuteResult<List<Map<String, Object>>>().result(list);
        consumer.accept(executeResult);
    }


}
