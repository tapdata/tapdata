package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.util.ConnectorUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static io.tapdata.base.ConnectorBase.list;

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

    private final static String EXPORT_JAVA_HOME = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";

    private final static String START_CDC_0 = "%s/bin/replicant %s %s/config/sybase2csv/src_sybasease.yaml %s/config/sybase2csv/dst_localstorage.yaml --general %s/config/sybase2csv/general.yaml --filter %s --extractor %s/config/sybase2csv/ext_sybasease.yaml --id %s --replace %s";     // --verbose

    //private final static String START_CDC_0 = "%s/bin/replicant %s %s/config/sybase2csv/src_sybasease.yaml %s/config/sybase2csv/dst_localstorage.yaml --general %s/config/sybase2csv/general.yaml --filter %s --extractor %s/config/sybase2csv/ext_sybasease.yaml --applier %s/config/sybase2csv/localstrange.yaml --id %s --replace %s";     // --verbose

    public final static String RE_INIT_AND_ADD_TABLE = START_CDC_0 + " --reinitialize %s/config/sybase2csv/task/%s/sybasease_reinit.yaml";

    @Override
    public synchronized CdcRoot compile() {
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
        //String cmd = START_CDC
        //        .replace("$taskId$", root.getCdcId())
        //        .replaceAll("\\$pocCliPath\\$", root.getCliPath())
        //        .replaceAll("\\$pocPath\\$", sybasePocPath)
        //        .replace("$commandType$", CommandType.type(commandType))
        //        .replace("$overwriteType$", "--" + OverwriteType.type(overwriteType));
        root.getContext().getLog().info("shell is {}", cmd);
        try {
            Thread.sleep(500);
            String[] cmds = new String[]{"/bin/sh", "-c", EXPORT_JAVA_HOME + "; " + cmd};
            Process exec = run(cmds);
            if (null == exec) {
                throw new CoreException("Cdc tool can not running, fail to get stream data");
            }
            String name = exec.getClass().getName();
            long cdcPid = -1;
            Class<? extends Process> aClass = exec.getClass();
            KVMap<Object> stateMap = root.getContext().getStateMap();
            stateMap.put("tableOverType", OverwriteType.RESUME.getType());
            try {
                if ("java.lang.UNIXProcess".equals(name)) {
                    Field pid = aClass.getDeclaredField("pid");
                    pid.setAccessible(true);
                    cdcPid = pid.getLong(pid);
                    stateMap.put("cdcPid", cdcPid);
                } else if ("java.lang.ProcessImpl".equals(name)) {
                    Field pid = aClass.getDeclaredField("handle");
                    pid.setAccessible(true);
                    cdcPid = (Integer) pid.get(pid);
                    stateMap.put("cdcPid", cdcPid);
                } else {
                    root.getContext().getLog().info("Cdc tool is running, but can not get it's pid, {}, {}", aClass.getName());
                }
            } catch (Exception ignore) { }
            if (cdcPid > 0) {
                root.getContext().getLog().info("Cdc tool is running which pid is {}", cdcPid);
            } else {
                root.getContext().getLog().info("Cdc tool is running, but can not get it's pid, {}, {}", aClass.getName());
            }
        } catch (Exception e) {
            throw new CoreException("Command exec failed, unable to start cdc command: {}, msg: {}", cmd, e.getMessage());
        } finally {
            root.getContext().getLog().info("You can cat {}/config/sybase2csv/trace/{}/trace.log to view the log information generated during the corresponding cdc execution",
                    sybasePocPath, processId);
        }
        return this.root;
    }

    public static final int RUN_TOOL_FAIL = 3624815;

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
