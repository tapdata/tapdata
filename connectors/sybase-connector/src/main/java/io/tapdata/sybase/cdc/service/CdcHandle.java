package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.LivenessMonitor;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.cdc.dto.start.SybaseDstLocalStorage;
import io.tapdata.sybase.cdc.dto.start.SybaseExtConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseGeneralConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Code;
import io.tapdata.sybase.util.Utils;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.map;

/**
 * @author GavinXiao
 * @description StartCdc create by Gavin
 * @create 2023/7/13 11:49
 **/
public class CdcHandle {
    CdcRoot root;
    StopLock lock;
    TapConnectorContext context;
    FileMonitor fileMonitor;
    final Object closeLock = new Object();
    ListenFile listenFile;

    public CdcHandle(CdcRoot root, TapConnectorContext context, StopLock lock) {
        this.root = root;
        this.lock = lock;
        this.context = context;
        this.root.setContext(context);
    }

    public CdcHandle streamReadConsumer(StreamReadConsumer cdcConsumer, Log log, String monitorPath) {
        this.fileMonitor = new FileMonitor(cdcConsumer, 1000, log, monitorPath);
        return this;
    }

    //Step #1
    public synchronized void startCdc(OverwriteType overwriteType) {
        ConfigBaseField baseField = new ConfigBaseField(root, "");
        if (!baseField.checkStep()) {
            this.root = baseField.compile();
        }
        //String pocPath = compileBaseFile.getSybasePocPath();
        compileYamlConfig();
        CdcStartVariables variables = root.getVariables();
        CdcRoot compileYaml = new ConfigYaml(this.root, variables).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
        //root.getContext().getLog().warn(port(new String[]{ "/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli" }, list("grep sybase-poc/replicant-cli")).toString());
    }

    private synchronized void compileYamlConfig() {
        String sybasePocPath = root.getSybasePocPath();
        NodeConfig nodeConfig = new NodeConfig(root.getContext());

        //@todo set CdcStartVariables from context config
        CdcStartVariables startVariables = CdcStartVariables.create();
        ConnectionConfig connectionConfig = new ConnectionConfig(context);


        SybaseSrcConfig srcConfig = new SybaseSrcConfig();
        srcConfig.setType("SYBASE_ASE");
        srcConfig.setDatabase(connectionConfig.getDatabase());
        srcConfig.setHost(connectionConfig.getHost());
        srcConfig.setPort(connectionConfig.getPort());
        srcConfig.setPassword(connectionConfig.getPassword());
        srcConfig.setUsername(connectionConfig.getUsername());
        srcConfig.setMax_connections(2);
        srcConfig.setMax_retries(10);
        srcConfig.setRetry_wait_duration_ms(1000);
        srcConfig.setTransaction_store_location("" + sybasePocPath + "/config/sybase2csv/data");
        srcConfig.setTransaction_store_cache_limit(100000);


        SybaseDstLocalStorage dstLocalStorage = new SybaseDstLocalStorage();
        dstLocalStorage.setStorage_location("" + sybasePocPath + "/config/sybase2csv/csv");
        dstLocalStorage.setFile_format("CSV");
        dstLocalStorage.setType("LOCALSTORAGE");

        SybaseGeneralConfig generalConfig = new SybaseGeneralConfig();
        LivenessMonitor monitor = new LivenessMonitor();
        monitor.setEnable(false);
        monitor.setInactive_timeout_ms(900_000);
        monitor.setMin_free_memory_threshold_percent(5);
        monitor.setLiveness_check_interval_ms(60_000);
        generalConfig.setLiveness_monitor(monitor);
        generalConfig.setTrace_dir("" + sybasePocPath + "/config/sybase2csv/trace");
        generalConfig.setData_dir("" + sybasePocPath + "/config/sybase2csv/data");
        //generalConfig.setError_connection_tracing("" + sybasePocPath + "/config/sybase2csv/trace");
        generalConfig.setLicense_path(root.getCliPath() + "/");
        generalConfig.setError_trace_dir("" + sybasePocPath + "/config/sybase2csv/trace");

        SybaseExtConfig extConfig = new SybaseExtConfig();
        SybaseExtConfig.Realtime realtime = extConfig.getRealtime();
        realtime.setFetchIntervals(nodeConfig.getFetchInterval());
        if (nodeConfig.isHeartbeat()) {
            String hbDatabase = nodeConfig.getHbDatabase();
            String hbSchema = nodeConfig.getHbSchema();
            SybaseExtConfig.Realtime.Heartbeat heartbeat = new SybaseExtConfig.Realtime.Heartbeat();
            heartbeat.setEnable(true);
            heartbeat.setInterval_ms(10000L);
            heartbeat.setCatalog(hbDatabase);
            heartbeat.setSchema(hbSchema);
            realtime.setHeartbeat(heartbeat);
            root.getContext().getLog().info("Heartbeat is open which has created at {}.{} , please ensure", hbDatabase, hbSchema);
        }

        this.root.setVariables(
                startVariables
                        .extConfig(extConfig)
                        .filterConfig(compileFilterTableYamlConfig(connectionConfig))
                        .srcConfig(srcConfig)
                        .sybaseDstLocalStorage(dstLocalStorage)
                        .sybaseGeneralConfig(generalConfig)
        );
    }

    public synchronized List<SybaseFilterConfig> compileFilterTableYamlConfig(ConnectionConfig connectionConfig) {
        List<SybaseFilterConfig> filterConfigs = new ArrayList<>();
        SybaseFilterConfig filterConfig = new SybaseFilterConfig();
        filterConfig.setCatalog(connectionConfig.getDatabase());
        filterConfig.setSchema(connectionConfig.getSchema());
        filterConfig.setTypes(list("TABLE", "VIEW"));

        List<String> cdcTables = root.getCdcTables();
        if (null == cdcTables || cdcTables.isEmpty()) {
            throw new CoreException(Code.STREAM_READ_WARN, "Not any table need to cdc");
        }
        Map<String, Object> tables = map();
        for (String cdcTable : cdcTables) {
            root.getContext().getLog().info("table: {}, contains timestamp: {}", cdcTable, root.getContainsTimestampFieldTables().contains(cdcTable));
            tables.put(cdcTable, null != root.getContainsTimestampFieldTables() && root.getContainsTimestampFieldTables().contains(cdcTable) ? SybaseFilterConfig.ignoreColumns() : SybaseFilterConfig.unIgnoreColumns());
        }
        filterConfig.setAllow(tables);
        filterConfigs.add(filterConfig);
        CdcStartVariables variables = this.root.getVariables();
        if (null != variables) {
            variables.filterConfig(filterConfigs);
        }
        return filterConfigs;
    }

    public synchronized void initCdc(OverwriteType overwriteType) {
        CdcRoot compileBaseFile = new ConfigBaseField(root, "").compile();
        //String pocPath = compileBaseFile.getSybasePocPath();
        compileYamlConfig();
        CdcRoot compileYaml = new ConfigYaml(compileBaseFile, root.getVariables()).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
        //root.getContext().getLog().warn(port(new String[]{ "/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli" }, list("grep sybase-poc/replicant-cli")).toString());
    }

    public synchronized void refreshCdc(OverwriteType overwriteType) {
        stopCdc();
        startCdc(overwriteType);
    }

    //Step #2
    public synchronized CdcPosition startListen(
            String monitorPath,
            String monitorFileName,
            List<String> tables,
            CdcPosition position,
            int batchSize,
            StreamReadConsumer consumer) {
        if (null == position) position = new CdcPosition();
        streamReadConsumer(consumer, context.getLog(), monitorPath);
        listenFile = new ListenFile(this.root,
                monitorPath,
                tables,
                monitorFileName,
                new AnalyseCsvFile(this.root, position, null),
                lock,
                batchSize
        ).monitor(fileMonitor);
        listenFile.compile();
        return position;
    }

    //Step #end 1
    public synchronized void releaseCdc() {
        if (null != listenFile) listenFile.onStop();
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        Object cdcPath = "";
        try {
            if (context != null) {
                KVMap<Object> stateMap = context.getStateMap();
                cdcPath = stateMap.get("cdcPath");
                if (null == cdcPath || "/*".equals(cdcPath) || "".equals(cdcPath.toString().trim())) {
                    return;
                }
                File file = new File(String.valueOf(cdcPath));
                if ("linux".equalsIgnoreCase(System.getProperty("os.name"))) {
                    final String shell = "rm -rf " + cdcPath;
                    root.getContext().getLog().info("clean cdc path: {}", shell);
                    root.getContext().getLog().info(Utils.run(shell));
                    if (file.exists()) {
                        FileUtils.delete(file);
                    }
                } else {
                    FileUtils.delete(file);
                }
            }
        } catch (Exception e) {
            Object finalCdcPath = cdcPath;
            Optional.ofNullable(context.getLog()).ifPresent(log -> log.warn("Can not release cdc path, please go to path: {}, and clean the file", finalCdcPath));
        }
        //Optional.ofNullable()
    }

    //Step #end 2
    public synchronized void stopCdc() {
        if (null != listenFile) listenFile.onStop();
        //@todo
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);

        String hostPortFromConfig = CdcHandle.getCurrentInstanceHostPortFromConfig(context);
        String targetPath = "sybase-poc-temp/" + hostPortFromConfig + "/";
        safeStopShell(context.getLog(), targetPath);
        //@todo
        root.setProcess(null);
        try {
            Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        } catch (Exception e) {
            root.getContext().getLog().info(e.getMessage());
        }

        NodeConfig nodeConfig = new NodeConfig(context);
        try {
            //缓冲作用，延时停止，等待数据库进程释放
            closeLock.wait(nodeConfig.getCloseDelayMill());
        } catch (Exception ignored) {

        }
    }

    public static void safeStopShell(Log log, String targetPath) {
        try {
            safeStopShell(log, port(log, new String[]{"/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli | grep " + targetPath }, list("grep sybase-poc/replicant-cli")), targetPath);
        } catch (Exception e) {
            if (null != log) log.warn("Can not auto stop cdc tool, please go to server and kill process by shell {} and after find process PID by shell {}",
                    "kill pid1 pid2 pid3 ",
                    "ps -ef|grep sybase-poc/replicant-cli");
        }
    }

    public static void safeStopShell(Log log, List<Integer> port, String targetPath) {
        try {
            if (!port.isEmpty()) {
                stopShell("-15", log, port);
                Thread.sleep(5000);
                port = port(log, new String[]{"/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli | grep " + targetPath }, list("grep sybase-poc/replicant-cli"));
                if (!port.isEmpty()) {
                    stopShell("-9", log, port);
                    Thread.sleep(5000);
                }
            }
        } catch (Exception e) {
            if (null != log) log.warn("Can not auto stop cdc tool, please go to server and kill process by shell {} and after find process PID by shell {}",
                    "kill pid1 pid2 pid3 ",
                    "ps -ef|grep sybase-poc/replicant-cli");
        }
    }

    private static void stopShell(String killType, Log log, List<Integer> port) {
        //String cmd = "ps -ef|grep sybase-poc/replicant-cli";
        ///bin/sh -c export JAVA_TOOL_OPTIONS="-Duser.language=en"; /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        //sh /tapdata/apps/sybase-poc/replicant-cli/bin/replicant real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        //java -Duser.timezone=UTC -Djava.system.class.loader=tech.replicant.util.ReplicantClassLoader -classpath /tapdata/apps/sybase-poc/replicant-cli/target/replicant-core.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts-5089.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/ts.jar:/tapdata/apps/sybase-poc/replicant-cli/lib/* tech.replicant.Main real-time /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/src_sybasease.yaml /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/dst_localstorage.yaml --general /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/general.yaml --filter /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/filter_sybasease.yaml --extractor /tapdata/apps/sybase-poc-temp/b5a9c529fd164b5/sybase-poc/config/sybase2csv/ext_sybasease.yaml --id b5a9c529fd164b5 --replace --overwrite --verbose
        if (null != port && !port.isEmpty()) {
            StringJoiner joiner = new StringJoiner(" ");
            for (Integer portNum : port) {
                joiner.add("" + portNum);
            }
            if (null != log) log.debug("All cdc replicant process's pid: {}", port.toString());
            execCmd(log, "kill " + (null != killType && "".equals(killType.trim()) ? killType + " " : "") + joiner.toString());
        }
    }


    public static List<Integer> port(Log log, String[] cmd, List<String> ignoreShells) {
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
                    log.debug(line);
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
                        } catch (Exception ignore) { }
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
                if (!execFlag) {
                    throw new RuntimeException(sb.toString());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
            //log.error("执行失败",e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) { }
            }
        }
        //port.sort(Comparator.comparingInt(o -> o));
        return port;
    }

    public static String execCmd(Log log, String cmd) {
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
            if (null != log)  log.warn("Can not auto stop cdc tool, please go to server and kill process by shell {} and after find process PID by shell {}",
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


    public CdcRoot getRoot() {
        return root;
    }

    public void setRoot(CdcRoot root) {
        this.root = root;
    }

    public static String getCurrentInstanceHostPortFromConfig(TapConnectorContext context) {
        ConnectionConfig config = new ConnectionConfig(context);
        final String host = config.getHost();
        int port = config.getPort();
        return host + ":" + port;
    }
}
