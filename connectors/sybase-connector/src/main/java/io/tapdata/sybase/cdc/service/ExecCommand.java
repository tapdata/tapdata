package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;

import java.io.BufferedReader;
import java.io.File;
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


    //private final static String RESTART_CDC = "%s/bin/replicant --reinitialize %s/config/filter_sybasease.yaml";

    private final static String EXPORT_JAVA_HOME = "export JAVA_TOOL_OPTIONS=\"-Duser.language=en\"";
    public final static String START_CDC = "$pocCliPath$/bin/replicant $commandType$ $pocPath$/config/sybase2csv/src_sybasease.yaml $pocPath$/config/sybase2csv/dst_localstorage.yaml --general $pocPath$/config/sybase2csv/general.yaml --filter $pocPath$/config/sybase2csv/filter_sybasease.yaml --extractor $pocPath$/config/sybase2csv/ext_sybasease.yaml --id $taskId$ --replace $overwriteType$ --verbose";


    public ExecCommand(CdcRoot root, CommandType commandType, OverwriteType overwriteType) {
        this.root = root;
        this.commandType = commandType;
        this.overwriteType = overwriteType;
        isRunCdc = false;
    }

    private final static String START_CDC_0 = "%s/bin/replicant %s %s/config/sybase2csv/src_sybasease.yaml %s/config/sybase2csv/dst_localstorage.yaml --general %s/config/sybase2csv/general.yaml --filter %s --extractor %s/config/sybase2csv/ext_sybasease.yaml --id %s --replace %s --verbose";

    public final static String RE_INIT_AND_ADD_TABLE = START_CDC_0 + " --reinitialize %s/config/sybase2csv/task/%s/sybasease_reinit.yaml";

    @Override
    public synchronized CdcRoot compile() {
        //if (isRunCdc) return root;
        String sybasePocPath = root.getSybasePocPath();
        String cmd = String.format(START_CDC_0,
                root.getCliPath(),
                CommandType.type(commandType),
                sybasePocPath,
                sybasePocPath,
                sybasePocPath,
                root.getFilterTableConfigPath(),
                sybasePocPath,
                root.getCdcId(),
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
            String[] cmds = new String[]{
                    "/bin/sh",
                    "-c",
                    EXPORT_JAVA_HOME + "; " + cmd
            };
            Process exec = run(cmds);
            if (null == exec) {
                throw new CoreException("Cdc tool can not running, fail to get stream data");
            }
            //isRunCdc = true;
            String name = exec.getClass().getName();
            long cdcPid = -1;
            Class<? extends Process> aClass = exec.getClass();
            KVMap<Object> stateMap = root.getContext().getStateMap();
            KVMap<Object> globalStateMap = root.getContext().getGlobalStateMap();
            globalStateMap.put("tableOverType", OverwriteType.RESUME.getType());
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

    public void safeStopShell() {
        try {
            stopShell(new String[]{"/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli"}, list("grep sybase-poc/replicant-cli"));
        } catch (Exception e) {
            root.getContext().getLog().warn("Can not auto stop cdc tool, please go to server and kill process by shell {} and after find process PID by shell {}",
                    "kill pid1 pid2 pid3 ",
                    "ps -ef|grep sybase-poc/replicant-cli");
        }
    }

    private void stopShell(String[] cmd, List<String> ignoreShells) {
        //String cmd = "ps -ef|grep sybase-poc/replicant-cli";
        ///bin/sh -c export JAVA_TOOL_OPTIONS="-Duser.language=en"; /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        //sh /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        //java -Duser.timezone=UTC -Djava.system.class.loader=tech.replicant.util.ReplicantClassLoader -classpath /tapdata/apps/sybase-poc/replicant-cli/target/replicant-core.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts-5089.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/* tech.replicant.Main real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        List<Integer> port = port(cmd, ignoreShells);
        if (!port.isEmpty()) {
            StringJoiner joiner = new StringJoiner(" ");
            for (Integer portNum : port) {
                joiner.add("" + portNum);
            }
            root.getContext().getLog().warn(port.toString());
            execCmd("kill " + joiner.toString());
        }
    }

    private List<Integer> port(String[] cmd, List<String> ignoreShells) {
        List<Integer> port = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        boolean execFlag = true;
        try {
            if ("linux".equalsIgnoreCase(System.getProperty("os.name"))) {
                Process p = Runtime.getRuntime().exec(cmd);
                p.waitFor();
                br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                while ((line = br.readLine()) != null) {
                    root.getContext().getLog().info(line);
                    boolean needIgnore = false;
                    if (!ignoreShells.isEmpty()) {
                        for (String ignoreShell : ignoreShells) {
                            if (line.contains(ignoreShell)) {
                                needIgnore = true;
                                break;
                            }
                        }
                    }
                    if (needIgnore) continue;
                    String[] split = line.split("( )+");
                    if (split.length > 2) {
                        String portStr = split[1];
                        try {
                            port.add(Integer.parseInt(portStr));
                        } catch (Exception ignore) {
                        }
                    }
                }
                br.close();
                br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                while ((line = br.readLine()) != null) {
                    sb.append(System.lineSeparator());
                    sb.append(line);
                    if (line.length() > 0) {
                        execFlag = false;
                    }
                }
                if (execFlag) {

                } else {
                    throw new RuntimeException(sb.toString());
                }
            } else {
                //throw new RuntimeException("不支持的操作系统类型");
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
            //log.error("执行失败",e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
        return port;
    }

    private String execCmd(String cmd) {
        StringBuilder sb = new StringBuilder();
        BufferedReader br = null;
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(System.lineSeparator());
                sb.append(line);
            }
            br.close();
            br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((line = br.readLine()) != null) {
                sb.append(System.lineSeparator());
                sb.append(line);
            }
        } catch (Exception e) {
            root.getContext().getLog().warn("Can not auto stop cdc tool, please go to server and kill process by shell {} and after find process PID by shell {}",
                    "kill pid1 pid2 pid3 ",
                    "ps -ef|grep sybase-poc/replicant-cli");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
        return sb.toString();
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
