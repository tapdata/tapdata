package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.*;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.*;
import java.util.*;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.map;

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

    public CdcHandle streamReadConsumer(StreamReadConsumer cdcConsumer, Log log, String monitorPath, long delay) {
        this.fileMonitor = new FileMonitor(cdcConsumer, delay < 1000 | delay > 200000 ? 3000 : delay, log, monitorPath);
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
        NodeConfig nodeConfig = new NodeConfig(context);

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
        srcConfig.setMax_connections(10);
        srcConfig.setMax_retries(10);
        srcConfig.setRetry_wait_duration_ms(1000);
        srcConfig.setTransaction_store_location(sybasePocPath + ConfigPaths.SYBASE_USE_DATA_DIR);
        srcConfig.setTransaction_store_cache_limit(100000);

        //if (nodeConfig.getLogCdcQuery() == ReadFilter.LOG_CDC_QUERY_READ_LOG) {
        srcConfig.setClient_charset("iso_1");
        //}

        SybaseDstLocalStorage dstLocalStorage = new SybaseDstLocalStorage();
        dstLocalStorage.setStorage_location(sybasePocPath + ConfigPaths.SYBASE_USE_CSV_DIR);
        dstLocalStorage.setFile_format("CSV");
        dstLocalStorage.setType("LOCALSTORAGE");

        SybaseGeneralConfig generalConfig = new SybaseGeneralConfig();
        LivenessMonitor monitor = new LivenessMonitor();
        monitor.setEnable(false);
        monitor.setInactive_timeout_ms(900_000);
        monitor.setMin_free_memory_threshold_percent(5);
        monitor.setLiveness_check_interval_ms(60_000);
        generalConfig.setLiveness_monitor(monitor);
        generalConfig.setTrace_dir(sybasePocPath + ConfigPaths.SYBASE_USE_TRACE_DIR);
        generalConfig.setData_dir(sybasePocPath + ConfigPaths.SYBASE_USE_DATA_DIR);
        generalConfig.setLicense_path(root.getCliPath() + "/");
        generalConfig.setError_trace_dir(sybasePocPath + ConfigPaths.SYBASE_USE_TRACE_DIR);

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
            root.getContext().getLog().info("Heartbeat is open, please ensure heartbeat table exists in {}:{}/{}.{}, create SQL: CREATE TABLE \"{}\".\"{}\".\"replicate_io_cdc_heartbeat\"(\"timestamp\" BIGINT NOT NULL, PRIMARY KEY(\"timestamp\"));",
                    connectionConfig.getHost(),
                    connectionConfig.getPort(),
                    hbDatabase, hbSchema,
                    hbDatabase, hbSchema);
        }

        //SybaseLocalStrange sybaseLocalStrange = new SybaseLocalStrange();
        //sybaseLocalStrange.setSnapshotThreads(16)
        //        .setSnapshotTxSizeRows(1_000_00)
        //        .setRealtimeThreads(16)
        //        .setSnapshotTxSizeRows(1_000_00)
        //        .setRealtimeEncodeBinaryToBase64(true);

        this.root.setVariables(
                startVariables
                        .extConfig(extConfig)
                        .filterConfig(compileFilterTableYamlConfig0(root.getCdcTables()))
                        .srcConfig(srcConfig)
                        .sybaseDstLocalStorage(dstLocalStorage)
                        .sybaseGeneralConfig(generalConfig)
                        //.sybaseLocalStrange(sybaseLocalStrange)
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
            long delay,
            StreamReadConsumer consumer) {
        if (null == position) position = new CdcPosition();
        streamReadConsumer(consumer, context.getLog(), monitorPath, delay);
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
        KVMap<Object> stateMap = context.getStateMap();
        Object cdcPath = stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_KEY);
        try {
            if (context != null) {
                ConnectorUtil.deleteFile(String.valueOf(cdcPath), context.getLog());
            }
        } catch (Exception e) {
            context.getLog().warn("Can not release cdc path, please go to path: {}, and clean the file", cdcPath);
        }
        //Optional.ofNullable()
    }

    public synchronized void releaseTaskResources() {
        if (null != listenFile) listenFile.onStop();
        KVMap<Object> stateMap = context.getStateMap();
        Object cdcPath = stateMap.get(ConfigPaths.SYBASE_USE_TASK_CONFIG_KEY);
        try {
            if (context != null) {
                ConnectorUtil.deleteFile(String.valueOf(cdcPath), context.getLog());
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
        List<Map<String, Object>> sybaseFilter = configYaml.configSybaseFilter(filterTableYamlConfig);
        if (null != appendTables && !appendTables.isEmpty()) {
            //配置 reInit.yaml
            List<SybaseReInitConfig> initTables = compileReInitTableYamlConfig(appendTables, log);
            //写入 reInit.yaml
            List<LinkedHashMap<String, Object>> initTable = configYaml.configReInitTable(initTables);

            //执行命令
            String sybasePocPath = root.getSybasePocPath();
            try {
                String command = String.format(ExecCommand.RE_INIT_AND_ADD_TABLE,
                        root.getCliPath(),
                        CommandType.CDC.getType(),
                        sybasePocPath,
                        sybasePocPath,
                        sybasePocPath,
                        root.getFilterTableConfigPath(),
                        sybasePocPath,
                        ConnectorUtil.maintenanceGlobalCdcProcessId(root.getContext()),
                        sybasePocPath,
                        root.getTaskCdcId(),
                        "--" + OverwriteType.RESUME.getType()
                );
                root.getContext().getLog().info("shell reinit is {}", command);
                ConnectorUtil.execCmd(command,
                        "Fail to reInit when an new task start with new tables, msg: {}",
                        root.getContext().getLog(), false);
            } finally {
                root.getContext().getLog().info("Cdc process has restart...");
            }
        }
        //命令结束后，写入filter.yaml
        try {
            Thread.sleep(30000);
        } catch (Exception ignore) {} finally {
            ConnectorUtil.quickStopShell(context);
        }
        //重启任务
        new ExecCommand(root, CommandType.CDC, OverwriteType.RESUME).compile();
        return sybaseFilter;
    }

    public List<Map<String, Object>> compileFilterTableYamlConfig(Map<String, Map<String, List<String>>> appendTables) {
        return ConnectorUtil.fixYaml(compileFilterTableYamlConfig0(appendTables));
    }
    public List<SybaseFilterConfig> compileFilterTableYamlConfig0(Map<String, Map<String, List<String>>> appendTables) {
        List<SybaseFilterConfig> filterConfigs = new ArrayList<>();
        StringJoiner timestampExists = new StringJoiner(", ");
        TapConnectorContext context = root.getContext();
        KVReadOnlyMap<TapTable> tableMap = context.getTableMap();
        Iterator<Entry<TapTable>> tableIterator = tableMap.iterator();
        Map<String, TapTable> tapTableMap = new HashMap<>();
        try {
            while (tableIterator.hasNext()) {
                Entry<TapTable> next = tableIterator.next();
                tapTableMap.put(next.getKey(), next.getValue());
            }
        } catch (Exception e) {

        }

        Map<String, List<ConnectionConfigWithTables>> connectionConfigOfTable = ReadFilter.groupConnectionConfigWithTables(root);
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
                    String databaseSchema = String.format("%s.%s", database, schema);
                    for (String cdcTable : initTables) {
                        DataMap connectionConfig = ReadFilter.dispatchConnectionConfig(connectionConfigOfTable, databaseSchema, cdcTable);
                        List<String> blockFields = new ArrayList<>();
                        if (root.hasContainsTimestampFieldTables(database, schema, cdcTable)) {
                            timestampExists.add(database + "." + schema + "." + cdcTable);
                            blockFields.add("timestamp");
                        }
                        if (null != connectionConfig && !connectionConfig.isEmpty()) {
                            //&& ReadFilter.LOG_CDC_QUERY_READ_SOURCE == root.getNodeConfig().getLogCdcQuery()) {
                            //@todo
                            Integer logCdcQuery = connectionConfig.getInteger("logCdcQuery");
                            if (null != logCdcQuery && ReadFilter.LOG_CDC_QUERY_READ_SOURCE == logCdcQuery) {
                                final String tableFullName = String.format("%s.%s", databaseSchema, cdcTable);
                                Optional.ofNullable(tapTableMap.get(tableFullName)).ifPresent(tapTable -> {
                                    Collection<String> primaryKeys = tapTable.primaryKeys(true);
                                    if (null != primaryKeys && !primaryKeys.isEmpty()) {
                                        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
                                        if (null != nameFieldMap && !nameFieldMap.isEmpty()) {
                                            Set<String> blockFieldsSet = new HashSet<>();
                                            nameFieldMap.entrySet().stream()
                                                    .filter(Objects::nonNull)
                                                    .filter(f -> {
                                                        TapField tapField = f.getValue();
                                                        return !primaryKeys.contains(tapField.getName()) && SybaseFilterConfig.isBolField(tapField.getDataType());
                                                    }).forEach(f -> {
                                                String fieldName = f.getKey();
                                                blockFields.add(fieldName);
                                                blockFieldsSet.add(fieldName);
                                            });
                                            root.addExistsBlockFields(tableFullName, blockFieldsSet);
                                        }
                                    } else {
                                        root.getContext().getLog().debug("Not fund any primary key in table {} when config sybase filter yaml and has open read bol value from source, it's mean can not read from source of this table, auto read from log of this table now", cdcTable);
                                    }
                                });
                            }
                        }
                        tables.put(cdcTable, filterConfig.blockFieldName(blockFields));
                    }
                    filterConfig.setAllow(tables);
                    filterConfigs.add(filterConfig);
                }
            })
        );
        root.getContext().getLog().debug("The timestamp type field is included in the following table list: {}", timestampExists.toString());
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

    public boolean reflshCdcTable(Map<String, Map<String, List<String>>> tables) {
        if (null == listenFile) {
            root.getContext().getLog().warn("Refresh cdc table fail, listen monitor not aliveable");
            return false;
        }
        listenFile.reflshCdcTable(tables);
        return true;
    }

}
