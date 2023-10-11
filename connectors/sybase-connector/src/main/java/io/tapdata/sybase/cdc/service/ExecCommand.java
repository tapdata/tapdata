package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.csv.NormalFileReader;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.util.ConnectorUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author GavinXiao
 * @description ExecCommand create by Gavin
 * @create 2023/7/13 11:17
 **/
class ExecCommand implements CdcStep<CdcRoot> {
    private CdcRoot root;
    private CommandType commandType;
    private OverwriteType overwriteType;
    private boolean isRunCdc;
    //public final static String START_CDC = "$pocCliPath$/bin/replicant $commandType$ $pocPath$/config/sybase2csv/src_sybasease.yaml $pocPath$/config/sybase2csv/dst_localstorage.yaml --general $pocPath$/config/sybase2csv/general.yaml --filter $pocPath$/config/sybase2csv/filter_sybasease.yaml --extractor $pocPath$/config/sybase2csv/ext_sybasease.yaml --id $taskId$ --replace $overwriteType$ --verbose";

    public ExecCommand(CdcRoot root, CommandType commandType, OverwriteType overwriteType) {
        this.root = root;
        this.commandType = commandType;
        this.overwriteType = overwriteType;
        isRunCdc = false;
    }

    public final static String EXPORT_JAVA_OPTS = "export JAVA_OPTS='%s'";
    public final static String EXPORT_TOOL_OPTS = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";
    private final static String START_CDC_0 = "%s/bin/replicant %s %s/config/sybase2csv/src_sybasease.yaml %s/config/sybase2csv/dst_localstorage.yaml --general %s/config/sybase2csv/general.yaml --filter %s --extractor %s/config/sybase2csv/ext_sybasease.yaml --id %s --replace %s";     // --verbose
    public final static String RE_INIT_AND_ADD_TABLE =  "%s/bin/replicant %s %s/config/sybase2csv/src_sybasease.yaml %s/config/sybase2csv/dst_localstorage.yaml --general %s/config/sybase2csv/general.yaml --filter %s --extractor %s/config/sybase2csv/ext_sybasease.yaml --id %s --reinitialize %s/config/sybase2csv/task/%s/sybasease_reinit.yaml --replace %s";

    @Override
    public synchronized CdcRoot compile() {
        Log log = root.getContext().getLog();
        String sybasePocPath = root.getSybasePocPath();
        String processId = ConnectorUtil.maintenanceGlobalCdcProcessId(root.getContext());
        String cmd = String.format(START_CDC_0,
                root.getCliPath(),
                CommandType.type(commandType),
                sybasePocPath,
                sybasePocPath,
                sybasePocPath,
                root.getFilterTableConfigPath(),
                sybasePocPath,
                //sybasePocPath,
                processId,
                "--" + OverwriteType.type(overwriteType)
        );
        log.info("shell is {}", cmd);
        try {
            String[] cmds = new String[]{
                    "/bin/sh",
                    "-c",
                    String.format(EXPORT_JAVA_OPTS, root.getConnectionConfig().getToolJavaOptionsLine()) + "; " + EXPORT_TOOL_OPTS + "; " + cmd};
            Process exec = run(cmds);
            if (null == exec) {
                throw new CoreException("Cdc tool can not running, fail to get stream data");
            }

            try {
                Thread.sleep(10000);
            } catch (Exception ignore){}
            List<Integer> port = ConnectorUtil.port(
                    ConnectorUtil.getKillShellCmd(root.getContext()),
                    ConnectorUtil.ignoreShells,
                    log,
                    ConnectorUtil.getCurrentInstanceHostPortFromConfig(root.getContext())
            );
            KVMap<Object> stateMap = root.getContext().getStateMap();
            if (port.size() > 1) {
                stateMap.put("tableOverType", OverwriteType.RESUME.getType());
            } else {
                ConnectorUtil.showErrorTrance(log, sybasePocPath, processId);
            }
        } catch (Exception e) {
            throw new CoreException("Command exec failed, unable to start cdc command: {}, msg: {}", cmd, e.getMessage());
        } finally {
            log.info("You can cat {}/config/sybase2csv/trace/{}/trace.log to view the log information generated during the corresponding cdc execution",
                    sybasePocPath, processId);
        }
        return this.root;
    }

    public static final int RUN_TOOL_FAIL = 3624815;

    public void restartProcess(){
        String sybasePocPath = root.getSybasePocPath();
        try {
            String command = String.format(RE_INIT_AND_ADD_TABLE,
                    root.getCliPath(),
                    this.commandType.getType(),
                    sybasePocPath,
                    sybasePocPath,
                    sybasePocPath,
                    root.getFilterTableConfigPath(),
                    sybasePocPath,
                    ConnectorUtil.maintenanceGlobalCdcProcessId(root.getContext()),
                    sybasePocPath,
                    root.getTaskCdcId(),
                    "--" + this.overwriteType.getType()
            );
            String[] cmds = new String[]{
                    "/bin/sh",
                    "-c",
                    String.format(EXPORT_JAVA_OPTS, root.getConnectionConfig().getToolJavaOptionsLine()) + "; " + EXPORT_TOOL_OPTS + "; " + command};
            root.getContext().getLog().info("shell reinit is {}", command);
            ConnectorUtil.execCmd(
                    cmds,
                    root.getContext().getLog(),
                    false,
                    "Fail to reInit when an new task start with new tables, msg: {}");
        } finally {
            root.getContext().getLog().info("Cdc process has restart...");
        }
    }

    private Process run(String[] cmds) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        Process exec = runtime.exec(cmds);
        root.setProcess(exec);
        try {
            exec.exitValue();
        } catch (Exception e) {
            return exec;
        }
        throw new CoreException(RUN_TOOL_FAIL, "Cdc tool can not running, fail to get stream data");
    }

    public CdcRoot getRoot() {
        return root;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public OverwriteType getOverwriteType() {
        return overwriteType;
    }

    public boolean isRunCdc() {
        return isRunCdc;
    }

    public void setRunCdc(boolean runCdc) {
        isRunCdc = runCdc;
    }

    public void setRoot(CdcRoot root) {
        this.root = root;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public void setOverwriteType(OverwriteType overwriteType) {
        this.overwriteType = overwriteType;
    }
}
