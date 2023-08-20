package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseRecord;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseTapEventFromCsvString;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVOfBigFile;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVQuickly;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import io.tapdata.sybase.cdc.dto.watch.FileListener;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.ConfigPaths;
import io.tapdata.sybase.util.ConnectorUtil;
import io.tapdata.sybase.util.Utils;
import io.tapdata.sybase.util.YamlUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.list;

/**
 * @author GavinXiao
 * @description LinstenFile create by Gavin
 * @create 2023/7/13 11:32
 **/
public class ListenFile implements CdcStep<CdcRoot> {
    public static final String TAG = ListenFile.class.getSimpleName();
    private final CdcRoot root;
    private final String monitorPath; ///${host:port}/sybase-poc/config/csv
    private final StopLock lock;
    private final AnalyseCsvFile analyseCsvFile;
    private final String monitorFileName;
    private Map<String, Map<String, List<String>>> tables;
    private final AnalyseRecord<List<String>, TapRecordEvent> analyseRecord;
    private final int batchSize;
    private final String schemaConfigPath;
    private final ConnectionConfig config;
    private final NodeConfig nodeConfig;
    private StreamReadConsumer cdcConsumer;
    FileMonitor fileMonitor;
    //long lastHeartbeatTime = 0;
    final ReadCSVOfBigFile readCSVOfBigFile = new ReadCSVOfBigFile();

    protected ListenFile(CdcRoot root,
                         String monitorPath,
                         Map<String, Map<String, List<String>>> tables,
                         String monitorFileName,
                         AnalyseCsvFile analyseCsvFile,
                         StopLock lock,
                         int batchSize) {
        this.root = root;
        if (null == monitorPath || "".equals(monitorPath.trim())) {
            throw new CoreException("Monitor path name can not be empty.");
        }
        this.monitorPath = monitorPath;
        this.lock = lock;
        this.analyseCsvFile = analyseCsvFile;
        if (null == monitorFileName || "".equals(monitorFileName.trim())) {
            throw new CoreException("Monitor file name can not be empty.");
        }
        this.monitorFileName = monitorFileName;
        this.tables = tables;
        root.getContext().getLog().warn("init with monitor table: {}", tables);
        analyseRecord = new AnalyseTapEventFromCsvString();
        this.batchSize = batchSize;
        this.schemaConfigPath = root.getSybasePocPath() + ConfigPaths.SCHEMA_CONFIG_PATH;
        this.config = new ConnectionConfig(root.getContext());
        this.nodeConfig = new NodeConfig(root.getContext());
        readCSVOfBigFile.setLog(root.getContext().getLog());
        //currentFileNames = new ConcurrentHashMap<>();
    }

    public void onStop() {
        try {
            Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        } catch (Exception e) {
            root.getContext().getLog().debug("Cdc monitor stop fail, msg: {}", e.getMessage());
        }
    }


    public ListenFile monitor(FileMonitor monitor) {
        this.fileMonitor = monitor;
        this.cdcConsumer = monitor.getCdcConsumer();
        return this;
    }

    final Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap = new HashMap<>();
    @Override
    public CdcRoot compile() {
        if (null == fileMonitor) {
            throw new CoreException("File monitor not start, cdc is stop work.");
        }
        TapConnectorContext context = root.getContext();
        if (null == context) {
            throw new CoreException("Can not get tap connection context.");
        }
        //final KVReadOnlyMap<TapTable> tableMap = context.getTableMap();
        try {
            tableMap.putAll(getTableFromConfig(root.getCdcTables()));
        } catch (Exception e) {
            this.root.getContext().getLog().debug("Can not get Table from config before monitor start, msg: {}", e.getMessage());
        }
        AtomicBoolean hasHandelInit = new AtomicBoolean(false);

        final Integer cdcCacheTime = Optional.ofNullable(nodeConfig.getCdcCacheTime()).orElse(5) * 60000;
        //root.getContext().getLog().warn("monitor file: {}", monitorPath);

        fileMonitor.monitor(monitorPath, new FileListener() {

            private void foreachTable(boolean needDeleteCsvFile) {
                if (tables.isEmpty()) return;
                //遍历monitorPath 所有子目录下的
                for (Map.Entry<String, Map<String, List<String>>> databaseEntry : tables.entrySet()) {
                    String database = databaseEntry.getKey();
                    Map<String, List<String>> schemaMap = databaseEntry.getValue();
                    if (null == database || null == schemaMap || schemaMap.isEmpty())  continue;
                    final String tableSpace = monitorPath + "/" + database + "/";
                    Set<Map.Entry<String, List<String>>> schemaEntry = schemaMap.entrySet();
                    for (Map.Entry<String, List<String>> schemaMaps : schemaEntry) {
                        String schema = schemaMaps.getKey();
                        List<String> tablesMaps = schemaMaps.getValue();
                        if (null == schema || null == tablesMaps || tablesMaps.isEmpty()) continue;
                        final String tempDir = tableSpace + schema + "/";
                        for (String table : tablesMaps) {
                            if (null == table || "".equals(table.trim())) continue;
                            final String tempPath = tempDir + table;
                            File tableSpaceFile = new File(tempPath);
                            if (!tableSpaceFile.exists()) continue;
                            //root.getContext().getLog().warn("Csv files path: {}", tempPath);
                            Arrays.stream(Objects.requireNonNull(tableSpaceFile.listFiles()))
                                .filter(file -> Objects.nonNull(file) && file.exists() && file.isFile() && isCSV(file))
                                .sorted(Comparator.comparing(File::getName))
                                .forEach(file -> monitorCSVFileIfPresentDeleteWithConfigTime(file, cdcCacheTime, needDeleteCsvFile));
                        }
                    }
                }
            }

            @Override
            public void onStart(FileAlterationObserver observer) {
                if (null == cdcConsumer) return;
                //Log log = context.getLog();
                //String hostPortFromConfig = ConnectorUtil.getCurrentInstanceHostPortFromConfig(context);
                //if (System.currentTimeMillis() - lastHeartbeatTime > 150000) {
                //    List<Integer> port = ConnectorUtil.port(ConnectorUtil.getKillShellCmd(context), ConnectorUtil.ignoreShells, log, hostPortFromConfig);
                //    if (port.size() < 3) {
                //        log.info("The CDC process will be restarted: no incremental files were detected to have been created or modified for a long time (2.5 minutes). Please check if incremental data has occurred on the source side");
                //        //超过2.5分钟未检测到CSV文件变更，重启cdc工具
                //        ConnectorUtil.safeStopShell(context, port);
                //        new ExecCommand(root, CommandType.CDC, OverwriteType.RESUME).compile();
                //    }
                //    lastHeartbeatTime = System.currentTimeMillis();
                //}
                try {
                    super.onStart(observer);
                    if (!hasHandelInit.get() && !tables.isEmpty()) {
                        hasHandelInit.set(true);
                        foreachTable(false);
                    }
                } catch (Exception e) {
                    cdcConsumer.streamReadEnded();
                    throw new CoreException("Start monitor file failed, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onStop(FileAlterationObserver observer) {
                super.onStop(observer);
                try {
                    foreachTable(false);
                } catch (Exception e) {
                    context.getLog().warn("Failed exec stop once, msg: {}", e.getMessage());
                }

                List<Integer> port = ConnectorUtil.port(
                        ConnectorUtil.getKillShellCmd(context),
                        ConnectorUtil.ignoreShells,
                        context.getLog(),
                        ConnectorUtil.getCurrentInstanceHostPortFromConfig(context)
                );
                if (port.size() < 2) {
                    //CDC_PROCESS_FAIL_EXCEPTION_CODE
                    throw new CoreException(SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE, "Cdc task is normal, but not any cdc process is active, will retry to start cdc process");
                }
            }

            @Override
            public void onFileChange(File file0) {
                try {
                    if (isCSV(file0)) {
                        monitor(file0, tableMap);
                    }
                } catch (Exception e) {
                    context.getLog().error("Monitor file change failed, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onFileCreate(File file0) {
                try {
                    if (isCSV(file0)) {
                        monitor(file0, tableMap);
                    }
                } catch (Exception e) {
                    context.getLog().error("Monitor file change failed, msg: {}", e.getMessage());
                }
            }


            private boolean isCSV(File file) {
                if (Objects.nonNull(file) && file.exists() && file.isFile()) {
                    String absolutePath = file.getAbsolutePath();
                    int indexOf = absolutePath.lastIndexOf('.');
                    String fileType = absolutePath.substring(indexOf + 1);
                    return fileType.equalsIgnoreCase("csv");
                } else {
                    return false;
                }
            }

            private boolean monitor(File file, Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap) {
                //lastHeartbeatTime = System.currentTimeMillis();
                boolean isThisFile = false;
                String absolutePath = file.getAbsolutePath();

                //root.getContext().getLog().warn("monitor file once, {}", absolutePath);


                String csvFileName = file.getName();
                String[] split = csvFileName.split("\\.");
                if (split.length < 3) {
                    throw new CoreException("Can not get table name from cav name, csv name is: {}", csvFileName);
                }
                final String database = split[0];
                final String schema = split[1];
                final String tableName = split[2];
                //切换csv文件时删除之前的csv文件
                CdcPosition position = analyseCsvFile.getPosition();
                if (!tables.containsKey(database) || null == tables.get(database)) {
                    return false;
                }
                Map<String, List<String>> schemaTableMap = tables.get(database);
                if (!schemaTableMap.containsKey(schema) || null == schemaTableMap.get(schema)) {
                    return false;
                }
                List<String> currentTables = schemaTableMap.get(schema);
                if (null != tableName && currentTables.contains(tableName)) {
                    isThisFile = true;
                    //final TapTable tapTable = tableMap.get(tableName);
                    if (tableMap.isEmpty()) {
                        try {
                            tableMap.putAll(getTableFromConfig(tables));
                        } catch (Exception e){
                            root.getContext().getLog().info("Can not get Table from config, msg: {}", e.getMessage());
                        }
                    }
                    LinkedHashMap<String, TableTypeEntity> tapTable = tableMap.get(tableName);
                    if (null == tapTable) {
                        try {
                            tableMap.putAll(getTableFromConfig(tables));
                        } catch (Exception e){
                            root.getContext().getLog().info("Can not get Table from config, msg: {}", e.getMessage());
                        }
                        tapTable = tableMap.get(tableName);
                    }
                    if (null == tapTable || tapTable.isEmpty()) {
                        root.getContext().getLog().warn("Can not get table info from schemas.yaml of table {}", tableName);
                        return false;
                    }

                    final List<TapEvent>[] events = new List[]{new ArrayList<>()};
                    CdcPosition.PositionOffset positionOffset = position.get(database, schema, tableName);
                    if (null == positionOffset) {
                        positionOffset = new CdcPosition.PositionOffset();
                        position.add(database, schema, tableName, positionOffset);
                    }
                    CdcPosition.CSVOffset csvOffset = positionOffset.csvOffset(absolutePath);
                    if (null == csvOffset) {
                        csvOffset = new CdcPosition.CSVOffset();
                        csvOffset.setOver(false);
                        csvOffset.setLine(0);
                        positionOffset.csvOffset(absolutePath, csvOffset);
                    }
                    AtomicReference<LinkedHashMap<String, TableTypeEntity>> tapTableAto = new AtomicReference<>(tapTable);
                    AtomicReference<CdcPosition.CSVOffset> csvOffsetAto = new AtomicReference<>(csvOffset);
                    try {
                        analyseCsvFile.analyse(file.getAbsolutePath(), tapTable, (compile, firstIndex, lastIndex) -> {
                            LinkedHashMap<String, TableTypeEntity> tableItem = tapTableAto.get();
                            CdcPosition.CSVOffset offset = csvOffsetAto.get();
                            int lineItem = offset.getLine();
                            for (List<String> list : compile) {
                                TapEvent recordEvent;
                                try {
                                    recordEvent = analyseRecord.analyse(list, tableItem, tableName, config, nodeConfig, null);
                                } catch (Exception e) {
                                    root.getContext().getLog().warn("An cdc event failed to accept in {} of {}, error csv format, csv line: {}, msg: {}", tableName, absolutePath, list, e.getMessage());
                                    continue;
                                }
                                if (null != recordEvent) {
                                    //事件发生的时间早于任务启动时间，当前事件需要被忽略
                                    //if (lineItem <= 0 && ((TapRecordEvent) recordEvent).getReferenceTime() < position.getCdcStartTime()) {
                                    //    continue;
                                    //}
                                    List<String> options = new ArrayList<>();
                                    options.add(database);
                                    options.add(schema);
                                    options.add(tableName);
                                    ((TapRecordEvent) recordEvent).setNamespaces(options);
                                    events[0].add(recordEvent);
                                    lineItem = offset.addAndGet();
                                    if (events[0].size() == batchSize) {
                                        offset.setOver(false);
                                        cdcConsumer.accept(events[0], position);
                                        events[0] = new ArrayList<>();
                                    }
                                }
                            }
                            /**
                            if (lineItem <= lastIndex) {
                                for (int index = 0; index < csvFileLines; index++) {
                                    if ((firstIndex + index) < lineItem) {
                                        continue;
                                    }
                                    TapEvent recordEvent;
                                    try {
                                        //recordEvent = analyseRecord.analyse(compile.get(index), tableItem, tableName, config, nodeConfig, cdcInfo -> {
                                        //    //cdcInfo: 每条csv携带的增量事件信息 not null
                                        //    //过滤增量时间点小于当前任务的启动时间的数据
                                        //    Object timestamp = cdcInfo.get("timestamp");
                                        //    if (null == timestamp) return true;
                                        //    long cdcReference = -1;
                                        //    try {
                                        //        cdcReference = Long.parseLong((String) timestamp);
                                        //    } catch (Exception ignore) {
                                        //    }
                                        //    return position.getCdcStartTime() > -1 && cdcReference >= position.getCdcStartTime();
                                        //});
                                        recordEvent = analyseRecord.analyse(compile.get(index), tableItem, tableName, config, nodeConfig, null);
                                    } catch (Exception e) {
                                        root.getContext().getLog().warn("An cdc event failed to accept in {} of {}, error csv format, csv line: {}, msg: {}", tableName, absolutePath, compile.get(index), e.getMessage());
                                        continue;
                                    }
                                    if (null != recordEvent) {
                                        //事件发生的时间早于任务启动时间，当前事件需要被忽略
                                        //if (lineItem <= 0 && ((TapRecordEvent) recordEvent).getReferenceTime() < position.getCdcStartTime()) {
                                        //    continue;
                                        //}
                                        List<String> options = new ArrayList<>();
                                        options.add(database);
                                        options.add(schema);
                                        options.add(tableName);
                                        ((TapRecordEvent) recordEvent).setNamespaces(options);
                                        events[0].add(recordEvent);
                                        lineItem = offset.addAndGet();
                                        if (events[0].size() == batchSize) {
                                            offset.setOver(false);
                                            cdcConsumer.accept(events[0], position);
                                            events[0] = new ArrayList<>();
                                        }
                                    }
                                }
                            }
                             **/
                        }).compile(readCSVOfBigFile, csvOffset.getLine());
                    } finally {
                        if (!events[0].isEmpty()) {
                            csvOffset.setOver(true);
                            cdcConsumer.accept(events[0], position);
                            events[0] = new ArrayList<>();
                        }
                    }
            }
                return isThisFile;
            }

            private void monitorCSVFileIfPresentDeleteWithConfigTime(File file, Integer cdcCacheTime, boolean needClean) {
                //root.getContext().getLog().warn("start monitor file once, {}", file.getAbsolutePath());
                if (isRecentCSV(file, System.currentTimeMillis(), cdcCacheTime)) {
                    //root.getContext().getLog().warn("monitor file once, {}", file.getAbsolutePath());
                    monitor(file, tableMap);
                } else {
                    if (needClean) {
                        try {
                            FileUtils.delete(file);
                        } catch (Exception ignore) {
                            root.getContext().getLog().warn("Can not to delete cdc cache file in {} of table name: {}", file.getAbsolutePath());
                        }
                    }
                }
            }

            private boolean isRecentCSV(File file, long compileTime, Integer cdcCacheTime) {
                if (isCSV(file)) {
                    long lastModify = file.lastModified();
                    //long now = System.currentTimeMillis();
                    // .    .    .
                    if (compileTime <= (lastModify + cdcCacheTime)) {
                        //root.getContext().getLog().info("File: {}, last modify time: {}, now: {}, cdc cache time: {}, need delete: true", file.getName(), lastModify, compileTime, cdcCacheTime);
                        return true;
                    }
                }
                //root.getContext().getLog().warn("File: {}, last modify time: {}, now: {}, cdc cache time: {}", file.getName(), file.lastModified(), cdcCacheTime, cdcCacheTime);
                return false;
            }
        });

        try {
            fileMonitor.start();
        } catch (Throwable e) {
            fileMonitor.stop();
            if (e instanceof CoreException && SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE == ((CoreException)e).getCode()) {
                throw (CoreException)e;
            }
            throw new CoreException("Can not monitor cdc for sybase, msg: {}", e.getMessage());
        }
        return this.root;
    }

    private Map<String, LinkedHashMap<String, TableTypeEntity>> getTableFromConfig(Map<String, Map<String, List<String>>> tableId) {
        Map<String, LinkedHashMap<String, TableTypeEntity>> table = new LinkedHashMap<>();
        if (null == tableId || tableId.isEmpty()) return table;
        YamlUtil schemas = new YamlUtil(schemaConfigPath);
        List<Map<String, Object>> schemaList = (List<Map<String, Object>>) schemas.get("schemas");
        tableId.forEach((database, tableInfoMap) -> {
            if (null == tableInfoMap || tableInfoMap.isEmpty()) return;
            tableInfoMap.forEach((schema, monitorInfo) -> {
                try {
                    for (Map<String, Object> objectMap : schemaList) {
                        Object catalog = objectMap.get("catalog");
                        Object schemaItem = objectMap.get("schema");
                        if (null != catalog && null != schemaItem && catalog.equals(database) && schemaItem.equals(schema)) {
                            Object tables = objectMap.get("tables");
                            if (!(tables instanceof Collection)) continue;
                            ((Collection<Map<String, Object>>) tables).stream()
                                    .filter(map -> Objects.nonNull(map) && monitorInfo.contains(String.valueOf(map.get("name"))))
                                    .forEach(tableInfo -> {
                                        Object columns = tableInfo.get("columns");
                                        if (columns instanceof Collection) {
                                            String tableName = String.valueOf(tableInfo.get("name"));
                                            Collection<Map<String, Object>> columnsList = (Collection<Map<String, Object>>) columns;
                                            LinkedHashMap<String, TableTypeEntity> tableClo = new LinkedHashMap<>();
                                            columnsList.stream().filter(Objects::nonNull).forEach(clo -> {
                                                String name = String.valueOf(clo.get("name"));
                                                String type = String.valueOf(clo.get("type"));
                                                tableClo.put(name, new TableTypeEntity(type, name, Utils.parseLengthFromTypeName(type)));
                                            });
                                            table.put(tableName, tableClo);
                                        }
                                    });
                            break;
                        }
                    }
                } catch (Exception e) {
                    root.getContext().getLog().warn("Can not read file {} to get {}'s schemas, msg: {}", schemaConfigPath, tableId, e.getMessage());
                }
            });
        });
        return table;
    }

    public void reflshCdcTable(Map<String, Map<String, List<String>>> tables) {
        if (null != tables && !tables.isEmpty()) {
            this.tables = tables;
            try {
                tableMap.putAll(getTableFromConfig(tables));
            } catch (Exception e){
                root.getContext().getLog().info("Can not get Table from config, msg: {}", e.getMessage());
            }
            root.getContext().getLog().warn("change monitor table to: {}", tables);
        }
    }
}
