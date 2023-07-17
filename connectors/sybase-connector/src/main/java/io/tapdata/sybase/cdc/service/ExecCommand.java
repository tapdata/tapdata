package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.StringJoiner;

/**
 * @author GavinXiao
 * @description ExecCommand create by Gavin
 * @create 2023/7/13 11:17
 **/
class ExecCommand implements CdcStep<CdcRoot> {
    private CdcRoot root;
    private CommandType commandType;
    private OverwriteType overwriteType;

    private final static String EXPORT_JAVA_HOME = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";
    private final static String START_CDC = "$pocCliPath$/bin/tapdata_sybase_cdc_replicant $commandType$ $pocPath$/config/sybase2csv/src_sybasease.yaml $pocPath$/config/sybase2csv/dst_localstorage.yaml --general $pocPath$/config/sybase2csv/general.yaml --filter $pocPath$/config/sybase2csv/filter_sybasease.yaml --extractor $pocPath$/config/sybase2csv/ext_sybasease.yaml --id $taskId$ --replace $overwriteType$ --verbose";

    private final static String KILL_SHELL = "";//= "ps -ef|grep 'tapdata_sybase_cdc_replicant'|awk '{print $2}'|xargs kill -15";

    protected ExecCommand(CdcRoot root, CommandType commandType, OverwriteType overwriteType) {
        this.root = root;
        this.commandType = commandType;
        this.overwriteType = overwriteType;
    }


    // /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/e1936185_1abd_4423_8d0f_3eca98d8a4e6/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/e1936185_1abd_4423_8d0f_3eca98d8a4e6/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/e1936185_1abd_4423_8d0f_3eca98d8a4e6/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/e1936185_1abd_4423_8d0f_3eca98d8a4e6/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/e1936185_1abd_4423_8d0f_3eca98d8a4e6/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id tstcsv1 --replace --overwrite --verbose

    @Override
    public CdcRoot compile() {
        String sybasePocPath = root.getSybasePocPath();
        String cmd = START_CDC
                .replace("$taskId$", root.getCdcId())
                .replaceAll("\\$pocCliPath\\$", root.getCliPath())
                .replaceAll("\\$pocPath\\$", sybasePocPath)
                .replace("$commandType$", CommandType.type(commandType))
                .replace("$overwriteType$", "--" + OverwriteType.type(overwriteType));
        root.getContext().getLog().info("shell is {}", cmd);
        try {
            Thread.sleep(500);
            String[] cmds = new String[]{
                    "/bin/sh",
                    "-c",
                     EXPORT_JAVA_HOME + "; " + cmd
            };
            Runtime runtime = Runtime.getRuntime();
            Process exec = runtime.exec(cmds);
            root.setProcess(exec);
            String name = exec.getClass().getName();
            long cdcPid = -1;
            Class<? extends Process> aClass = exec.getClass();
            StringJoiner joiner = new StringJoiner(",");
            try {
                exec.exitValue();
                throw new CoreException("Cdc tool can not running, fail to get stream data");//Utils.readFromInputStream(exec.getErrorStream(), StandardCharsets.UTF_8));
            } catch (Exception ignore){}

            KVMap<Object> stateMap = root.getContext().getStateMap();
            stateMap.put("tableOverType", OverwriteType.RESUME.getType());
            try {
                if ("java.lang.UNIXProcess".equals(name)) {
                    Field[] declaredFields = aClass.getDeclaredFields();
                    for (Field declaredField : declaredFields) {
                        declaredField.setAccessible(true);
                        joiner.add(declaredField.getName());
                    }
                    root.getContext().getLog().info("All Fields: , {}, {}", aClass.getName(), joiner.toString());

                    Field pid = aClass.getDeclaredField("pid");
                    pid.setAccessible(true);
                    cdcPid = pid.getLong(pid);
                    stateMap.put("cdcPid", cdcPid);
                } else if ("java.lang.ProcessImpl".equals(name)) {
                    Field pid = aClass.getDeclaredField("handle");
                    pid.setAccessible(true);
                    cdcPid = (Integer)pid.get(pid);
                    stateMap.put("cdcPid", cdcPid);
                } else {
                    root.getContext().getLog().info("Cdc tool is running, but can not get it's pid, {}, {}", aClass.getName());
                }
            } catch (Exception ignore){
            }
            if (cdcPid > 0) {
                root.getContext().getLog().info("Cdc tool is running which pid is {}", cdcPid);
            } else {
                root.getContext().getLog().info("Cdc tool is running, but can not get it's pid, {}, {}", aClass.getName());
            }
            root.getContext().getLog().info("All Fields: ", joiner.toString());

        } catch (Exception e) {
            throw new CoreException("Command exec failed, unable to start cdc command: {}, msg: {}", cmd, e.getMessage());
        } finally {
            root.getContext().getLog().info("You can go to {}/config/sybase2csv/trace/{}/trace.log to view the log information generated during the corresponding cdc execution",
                    sybasePocPath, root.getCdcId());
        }

        return this.root;
    }

    private String shellOutput(InputStream inputStream){
        Scanner br = null;
        StringBuilder builder = new StringBuilder();
        try {
            br = new Scanner(new InputStreamReader(inputStream));
            String line = null;
            while (br.hasNextLine()) {
                line = br.nextLine();
                builder.append(line).append("\n");
            }
        } finally {
            if (null != br) {
                br.close();
            }
        }
        return br.toString();
    }
}
