package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.utils.CommonUtils;
import picocli.CommandLine;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@CommandLine.Command(
        description = "Test allTables method",
        subcommands = MainCli.class
)
public class DiscoverSchemaCli extends CommonCli {
    private static final String TAG = DiscoverSchemaCli.class.getSimpleName();

    @CommandLine.Option(names = { "-i", "--id" }, required = true, description = "Provide PDK id")
    private String pdkId;

    @CommandLine.Option(names = { "-g", "--group" }, required = true, description = "Provide PDK group")
    private String pdkGroup;

    @CommandLine.Option(names = { "-v", "--version" }, required = true, description = "Provide PDK buildNumber")
    private String version;

    @CommandLine.Option(names = { "-c", "--connectionConfig" }, required = false, description = "Provide PDK connection config json string")
    private String connectionConfigStr;

    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        try {
            DataMap dataMap = null;
            if(connectionConfigStr != null) {
                dataMap = JSON.parseObject(connectionConfigStr, DataMap.class);
            }
            ConnectionNode connectionNode = PDKIntegration.createConnectionConnectorBuilder()
                    .withAssociateId(UUID.randomUUID().toString())
                    .withGroup(pdkGroup)
                    .withVersion(version)
                    .withPdkId(pdkId)
                    .withConnectionConfig(dataMap)
                    .build();
            connectionNode.discoverSchema(null, 1000, new Consumer<List<TapTable>>() {
                @Override
                public void accept(List<TapTable> tables) {
                    for(TapTable table : tables) {
                        TapLogger.info(TAG, "Table: {}", table);
                    }
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            CommonUtils.logError(TAG, "AllTables failed", throwable);
        }
        return 0;
    }
    public void validateAndFill(DAGDescriber dataFlowDescriber) {
//        Validator.checkAllNotNull(ErrorCodes.CLI_MISSING_SOURCE_OR_TARGET, dataFlowDescriber.getSourceNode(), dataFlowDescriber.getTargetNode());
//
//        SourceNode sourceNode = dataFlowDescriber.getSourceNode();
//        if(sourceNode.getId() == null) {
//            sourceNode.setId(CommonUtils.uuid());
//        }
//        if(sourceNode.getSourceOptions() == null) {
//            sourceNode.setSourceOptions(new SourceOptions());
//        }
//
//        TargetNode targetNode = dataFlowDescriber.getTargetNode();
//        if(targetNode.getId() == null) {
//            targetNode.setId(CommonUtils.uuid());
//        }
//        if(targetNode.getTargetOptions() == null) {
//            targetNode.setTargetOptions(new TargetOptions());
//        }
//
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_DATA_SOURCE_OR_TABLE, sourceNode.getDataSource(), sourceNode.getTable());
//
//        DataSource sourceDataSource = sourceNode.getDataSource();
//        if(sourceDataSource.getId() == null) {
//            sourceDataSource.setId(CommonUtils.uuid());
//        }
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_CONNECTION_STRING_OR_TYPE, sourceDataSource.getConnectionString(), sourceDataSource.getType());
//
//        Table sourceNodeTable = sourceNode.getTable();
//        Validator.checkAllNotNull(ErrorCodes.CLI_SOURCE_NODE_MISSING_DATABASE_OR_NAME, sourceNodeTable.getDatabase(), sourceNodeTable.getName());
//
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_DATA_SOURCE_OR_TABLE, targetNode.getDataSource(), targetNode.getTable());
//
//        DataSource targetDataSource = targetNode.getDataSource();
//        if(targetDataSource.getId() == null) {
//            targetDataSource.setId(CommonUtils.uuid());
//        }
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_CONNECTION_STRING_OR_TYPE, targetDataSource.getConnectionString(), targetDataSource.getType());
//
//        Table targetTable = targetNode.getTable();
//        Validator.checkAllNotNull(ErrorCodes.CLI_TARGET_NODE_MISSING_DATABASE_OR_NAME, targetTable.getDatabase(), targetTable.getName());
//
//        ScriptNode scriptNode = dataFlowDescriber.getScriptNode();
//        if(scriptNode != null) {
//            if(scriptNode.getType() == null)
//                scriptNode.setType(ScriptConstants.Groovy);
//            if(scriptNode.getProcessMethod() == null)
//                scriptNode.setProcessMethod("process");
//            Validator.checkAllNotNull(ErrorCodes.CLI_SCRIPT_NODE_MISSING_CLASS_NAME_OR_ROOT_PATH, scriptNode.getClassName(), scriptNode.getRootPath());
//        }
//        logger.info("sourceNode {}", JSON.toJSONString(sourceNode, true));
//        logger.info("scriptNode {}", (scriptNode != null ? JSON.toJSONString(scriptNode, true) : "null"));
//        logger.info("targetNode {}", JSON.toJSONString(targetNode, true));
    }


}
