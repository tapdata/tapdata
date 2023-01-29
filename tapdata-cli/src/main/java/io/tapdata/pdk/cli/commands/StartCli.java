package io.tapdata.pdk.cli.commands;

import com.alibaba.fastjson.JSON;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.cli.CommonCli;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.workflow.engine.*;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.IOUtils;
import picocli.CommandLine;

import java.io.File;
import java.io.FileInputStream;

@CommandLine.Command(
        description = "Run one or more data flow configuration files",
        subcommands = MainCli.class
)
public class StartCli extends CommonCli {
    private static final String TAG = StartCli.class.getSimpleName();
    @CommandLine.Parameters(paramLabel = "FILE", description = "one ore more data flow configuration files")
    File[] files;
    @CommandLine.Option(names = { "-v", "--verbose" }, required = false, description = "Enable debug log")
    private boolean verbose = false;
    @CommandLine.Option(names = { "-h", "--help" }, usageHelp = true, description = "TapData cli help")
    private boolean helpRequested = false;

    public Integer execute() throws Exception {
        CommonUtils.setProperty("refresh_local_jars", "true");
        if(verbose)
            CommonUtils.setProperty("tap_verbose", "true");
        try {
            DataFlowEngine dataFlowEngine = DataFlowEngine.getInstance();
            dataFlowEngine.start();

            if(files != null) {
                for(File file : files) {
                    if(file.exists() && file.isFile()) {
                        try(FileInputStream fileInputStream = new FileInputStream(file)) {
                            String json = IOUtils.toString(fileInputStream);
                            DAGDescriber dataFlowDescriber = JSON.parseObject(json, DAGDescriber.class);
//                            validateAndFill(dataFlowDescriber);
                            TapDAG dag = dataFlowDescriber.toDag();
                            if(dag != null) {
                                JobOptions jobOptions = dataFlowDescriber.getJobOptions();
                                dataFlowEngine.startDataFlow(dag, jobOptions);
                            }
                        }
                    } else {
                        TapLogger.info(TAG, "File {} not found or not a file", file);
                    }
                }
            }
        } catch (Throwable throwable) {
            CommonUtils.logError(TAG, "Start failed", throwable);
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
