package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * @author GavinXiao
 * @description ExecCommand create by Gavin
 * @create 2023/7/13 11:17
 **/
class ExecCommand implements CdcStep<CdcRoot> {
    private final CdcRoot root;
    private final CommandType commandType;
    private final OverwriteType overwriteType;

    private final static String EXPORT_JAVA_HOME = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";
    private final static String START_CDC = "$pocCliPath$/bin/replicant $commandType$ $pocPath$/config/sybase2csv/src_sybasease.yaml $pocPath$/config/sybase2csv/dst_localstorage.yaml --general $pocPath$/config/sybase2csv/general.yaml --filter $pocPath$/config/sybase2csv/filter_sybasease.yaml --extractor $pocPath$/config/sybase2csv/ext_sybasease.yaml --id $taskId$ --replace $overwriteType$ --verbose";

    protected ExecCommand(CdcRoot root, CommandType commandType, OverwriteType overwriteType) {
        this.root = root;
        this.commandType = commandType;
        this.overwriteType = overwriteType;
    }

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
            Process exec = run(cmds);
//            for (int index = 0; index < 1;index++){
//                try {
//                    exec = run(cmds);
//                } catch (CoreException e){
            //if (e.getCode() != RUN_TOOL_FAIL || index == 2) {
//                        throw e;
            //}
            //root.getContext().getLog().warn("Failed to start cdc tool, it's start again after 3s, please wait");
            //root.wait(3000);
//                }
//            }
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
            } catch (Exception ignore) {
            }
            if (cdcPid > 0) {
                root.getContext().getLog().info("Cdc tool is running which pid is {}", cdcPid);
            } else {
                root.getContext().getLog().info("Cdc tool is running, but can not get it's pid, {}, {}", aClass.getName());
            }
        } catch (Exception e) {
            throw new CoreException("Command exec failed, unable to start cdc command: {}, msg: {}", cmd, e.getMessage());
        } finally {
            root.getContext().getLog().info("You can cat {}/config/sybase2csv/trace/{}/trace.log to view the log information generated during the corresponding cdc execution",
                    sybasePocPath, root.getCdcId());
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
            throw new CoreException(RUN_TOOL_FAIL, "Cdc tool can not running, fail to get stream data");//Utils.readFromInputStream(exec.getErrorStream(), StandardCharsets.UTF_8));
        } catch (Exception ignore) {
        }
        return exec;
    }

//    private String shellOutput(InputStream inputStream){
//        Scanner br = null;
//        StringBuilder builder = new StringBuilder();
//        try {
//            br = new Scanner(new InputStreamReader(inputStream));
//            String line = null;
//            while (br.hasNextLine()) {
//                line = br.nextLine();
//                builder.append(line).append("\n");
//            }
//        } finally {
//            if (null != br) {
//                br.close();
//            }
//        }
//        return br.toString();
//    }
}
