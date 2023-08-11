package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.cache.KVMap;
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
import io.tapdata.sybase.cdc.dto.start.SybaseReInitConfig;
import io.tapdata.sybase.cdc.dto.start.SybaseSrcConfig;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Code;
import io.tapdata.sybase.util.ConfigPaths;
import io.tapdata.sybase.util.ConnectorUtil;
import io.tapdata.sybase.util.Utils;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.map;
import static io.tapdata.entity.simplify.TapSimplify.entry;

/**
 * @author GavinXiao
 * @description StartCdc create by Gavin
 * @create 2023/7/13 11:49
 **/
public class CdcHandle {
    private CdcRoot root;
    private StopLock lock;
    private TapConnectorContext context;
    private FileMonitor fileMonitor;
    private final Object closeLock = new Object();
    private ListenFile listenFile;

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
        compileYamlConfig();
        CdcStartVariables variables = root.getVariables();
        CdcRoot compileYaml = new ConfigYaml(this.root, variables).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
    }

    private synchronized void compileYamlConfig() {
        String sybasePocPath = root.getSybasePocPath();

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
        srcConfig.setTransaction_store_location(sybasePocPath + ConfigPaths.SYBASE_USE_DATA_DIR);
        srcConfig.setTransaction_store_cache_limit(1000);

        SybaseDstLocalStorage dstLocalStorage = new SybaseDstLocalStorage();
        dstLocalStorage.setStorage_location(sybasePocPath + ConfigPaths.SYBASE_USE_CSV_DIR);
        dstLocalStorage.setFile_format("CSV");
        dstLocalStorage.setType("LOCALSTORAGE");

        SybaseGeneralConfig generalConfig = new SybaseGeneralConfig();
        LivenessMonitor monitor = new LivenessMonitor();
        monitor.setEnable(true);
        monitor.setInactive_timeout_ms(900_000);
        monitor.setMin_free_memory_threshold_percent(5);
        monitor.setLiveness_check_interval_ms(60_000);
        generalConfig.setLiveness_monitor(monitor);
        generalConfig.setTrace_dir(sybasePocPath + ConfigPaths.SYBASE_USE_TRACE_DIR);
        generalConfig.setData_dir(sybasePocPath + ConfigPaths.SYBASE_USE_DATA_DIR);
        generalConfig.setLicense_path(root.getCliPath() + "/");
        generalConfig.setError_trace_dir(sybasePocPath + ConfigPaths.SYBASE_USE_TRACE_DIR);

        NodeConfig nodeConfig = new NodeConfig(context);
        SybaseExtConfig extConfig = new SybaseExtConfig();
        SybaseExtConfig.Realtime realtime = extConfig.getRealtime();
        realtime.setFetchIntervals(nodeConfig.getFetchInterval());

        this.root.setVariables(
                startVariables
                        .extConfig(extConfig)
                        .filterConfig(compileFilterTableYamlConfig0(root.getCdcTables()))
                        .srcConfig(srcConfig)
                        .sybaseDstLocalStorage(dstLocalStorage)
                        .sybaseGeneralConfig(generalConfig)
        );
    }

    public synchronized void initCdc(OverwriteType overwriteType) {
        CdcRoot compileBaseFile = new ConfigBaseField(root, "").compile();
        compileYamlConfig();
        CdcRoot compileYaml = new ConfigYaml(compileBaseFile, root.getVariables()).compile();
        new ExecCommand(compileYaml, CommandType.CDC, overwriteType).compile();
    }

    //Step #2
    public synchronized CdcPosition startListen(
            String monitorPath,
            String monitorFileName,
            Map<String, Map<String, List<String>>> tables,
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

    /**
     * @deprecated
     */
    public synchronized void releaseCdc() {
        if (null != listenFile) listenFile.onStop();
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        KVMap<Object> stateMap = context.getStateMap();
        Object cdcPath = stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_KEY);
        try {
            if (context != null) {
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
            context.getLog().warn("Can not release cdc path, please go to path: {}, and clean the file", cdcPath);
        }
        //Optional.ofNullable()
    }

    public synchronized void releaseTaskResources() {
        if (null != listenFile) listenFile.onStop();
        Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        KVMap<Object> stateMap = context.getStateMap();
        Object cdcPath = stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_KEY);
        try {
            if (context != null) {
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
            context.getLog().warn("Can not release cdc path, please go to path: {}, and clean and remove the file", cdcPath);
        }
    }

//Step #end 2
    public synchronized void stopCdc() {
        if (null != listenFile) listenFile.onStop();
        //@todo
        Optional.ofNullable(root.getProcess()).ifPresent(Process::destroy);

        ConnectorUtil.safeStopShell(context);
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
        } catch (Exception e) {

        }
    }

    public CdcRoot getRoot() {
        return root;
    }

    public void setRoot(CdcRoot root) {
        this.root = root;
    }

    public List<Map<String, Object>> addTableAndRestartProcess(ConnectionConfig config, Map<String, Map<String, List<String>>> allTables, Map<String, Map<String, List<String>>> appendTables, Log log) {
        //CdcRoot compileBaseFile = new ConfigBaseField(root, "").compile();
        ConfigYaml configYaml = new ConfigYaml(root, root.getVariables());
        //配置filter.yaml
        List<Map<String, Object>> filterTableYamlConfig = compileFilterTableYamlConfig(allTables);

        if (null != appendTables && !appendTables.isEmpty()) {
            //配置 reInit.yaml
            List<SybaseReInitConfig> initTables = compileReInitTableYamlConfig(appendTables, log);
            //写入 reInit.yaml
            List<LinkedHashMap<String, Object>> initTable = configYaml.configReInitTable(initTables);

            //执行命令
            String sybasePocPath = root.getSybasePocPath();
            ConnectorUtil.execCmd(String.format(ExecCommand.RE_INIT_AND_ADD_TABLE,
                    root.getCliPath(),
                    CommandType.CDC,
                    sybasePocPath,
                    sybasePocPath,
                    sybasePocPath,
                    root.getFilterTableConfigPath(),
                    sybasePocPath,
                    ConnectorUtil.maintenanceGlobalCdcProcessId(root.getContext()),
                    "--" + OverwriteType.RESUME.getType(),
                    sybasePocPath,
                    root.getTaskCdcId()
                    ),
                    "Fail to reInit when an new task start with new tables, msg: {}",
                    root.getContext().getLog());
        }
        //命令结束后，写入filter.yaml
        List<Map<String, Object>> sybaseFilter = configYaml.configSybaseFilter(filterTableYamlConfig);

        //重启任务
        new ExecCommand(root, CommandType.CDC, OverwriteType.RESUME).compile();
        return sybaseFilter;
    }

    public List<Map<String, Object>> compileFilterTableYamlConfig(Map<String, Map<String, List<String>>> appendTables) {
        return ConnectorUtil.fixYaml(compileFilterTableYamlConfig0(appendTables));
    }
    public List<SybaseFilterConfig> compileFilterTableYamlConfig0(Map<String, Map<String, List<String>>> appendTables) {
        List<SybaseFilterConfig> filterConfigs = new ArrayList<>();
        appendTables.forEach((database, mapInfo) -> mapInfo.forEach((schema, initTables) -> {
                if (null != initTables && !initTables.isEmpty()) {
                    if (null == database || null == schema) {
                        throw new CoreException("Unable get database or schema name, please set database or schema in connection config");
                    }
                    SybaseFilterConfig filterConfig = new SybaseFilterConfig();
                    filterConfig.setCatalog(database);
                    filterConfig.setSchema(schema);
                    filterConfig.setTypes(list("TABLE", "VIEW"));
                    Map<String, Object> tables = map();
                    for (String cdcTable : initTables) {
                        final boolean contains = root.hasContainsTimestampFieldTables(database, schema, cdcTable);
                        root.getContext().getLog().debug("table: {}, contains timestamp: {}", cdcTable, contains);
                        tables.put(cdcTable, contains ? ConnectorUtil.ignoreColumns() : ConnectorUtil.unIgnoreColumns());
                    }
                    filterConfig.setAllow(tables);
                    filterConfigs.add(filterConfig);
                }
            })
        );
        return filterConfigs;
    }

    public synchronized List<SybaseReInitConfig> compileReInitTableYamlConfig(Map<String, Map<String, List<String>>> appendTables, Log log) {
        List<SybaseReInitConfig> filterConfigs = new ArrayList<>();
        if (null != appendTables && !appendTables.isEmpty()) {
            appendTables.forEach((database, info) -> {
                if (null == info || info.isEmpty()) return;
                info.forEach((schema, tables) -> {
                    SybaseReInitConfig filterConfig = new SybaseReInitConfig();
                    filterConfig.setCatalog(database);
                    filterConfig.setSchema(schema);
                    filterConfig.setAdd_tables(tables);
                    filterConfigs.add(filterConfig);
                });
            });
        }
        CdcStartVariables variables = this.root.getVariables();
        if (null != variables) {
            variables.reInitConfigs(filterConfigs);
        }
        return filterConfigs;
    }
}
