package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseRecord;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseTapEventFromCsvString;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVOfBigFile;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.cdc.dto.watch.FileListener;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.Utils;
import io.tapdata.sybase.util.YamlUtil;
import io.tapdata.sybase.cdc.dto.start.CommandType;
import io.tapdata.sybase.cdc.dto.start.OverwriteType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private final String monitorPath; ///sybase-poc/config/sybase2csv/csv/testdb/tester
    private final StopLock lock;
    private final AnalyseCsvFile analyseCsvFile;
    private final String monitorFileName;
    private final List<String> tables;
    private final AnalyseRecord<List<String>, TapRecordEvent> analyseRecord;
    private final int batchSize;
    private final String schemaConfigPath;
    private final ConnectionConfig config;
    private final NodeConfig nodeConfig;
    private StreamReadConsumer cdcConsumer;
    FileMonitor fileMonitor;
    long lastHeartbeatTime = 0;

    protected ListenFile(CdcRoot root,
                         String monitorPath,
                         List<String> tables,
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
        analyseRecord = new AnalyseTapEventFromCsvString();
        this.batchSize = batchSize;
        this.schemaConfigPath = root.getSybasePocPath() + "/config/sybase2csv/csv/schemas.yaml";
        this.config = new ConnectionConfig(root.getContext());
        this.nodeConfig = new NodeConfig(root.getContext());
        //currentFileNames = new ConcurrentHashMap<>();
    }

    public void onStop() {
        try {
            Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
//            final Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap = getTableFromConfig(root.getCdcTables());
//            //currentFileNames = null;
//            if (null != tables) {
//                try {
//                    if (null != monitorPath) {
//                        File historyFile = new File(monitorPath);
//                        if (historyFile.exists()) {
//                            try {
//                                FileUtils.delete(historyFile);
//                            } catch (Exception e) {
//                                root.getContext().getLog().info("Can not to delete cdc cache file in {}", monitorPath);
//                            }
//                        }
//                    }
//                } catch (Exception e) {
//                    root.getContext().getLog().error("Cdc stop fail, can not remove monitor files, msg: {}", e.getMessage());
//                }
//            }
        } catch (Exception e) {
            root.getContext().getLog().warn("Cdc monitor stop fail, msg: {}", e.getMessage());
        }
    }


    public ListenFile monitor(FileMonitor monitor) {
        this.fileMonitor = monitor;
        this.cdcConsumer = monitor.getCdcConsumer();
        return this;
    }

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
        final Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap = getTableFromConfig(root.getCdcTables());
        AtomicBoolean hasHandelInit = new AtomicBoolean(false);

        final Integer cdcCacheTime = Optional.ofNullable(nodeConfig.getCdcCacheTime()).orElse(10) * 60000;
        fileMonitor.monitor(monitorPath, new FileListener() {
            @Override
            public void onStart(FileAlterationObserver observer) {
                Log log = context.getLog();
                if (System.currentTimeMillis() - lastHeartbeatTime > 150000) {
                    List<Integer> port = CdcHandle.port(log, new String[]{"/bin/sh", "-c", "ps -ef|grep sybase-poc/replicant-cli"}, list("grep sybase-poc/replicant-cli"));
                    if (null != port && port.size() < 3) {
                        context.getLog().info("The CDC process will be restarted: no incremental files were detected to have been created or modified for a long time (2.5 minutes). Please check if incremental data has occurred on the source side");
                        //超过2.5分钟未检测到CSV文件变更，重启cdc工具
                        CdcHandle.safeStopShell(log, port);
                        new ExecCommand(root, CommandType.CDC, OverwriteType.RESUME).compile();
                    }
                    lastHeartbeatTime = System.currentTimeMillis();
                }
                if (null == cdcConsumer) return;
                try {
                    super.onStart(observer);
                    if (hasHandelInit.get()) {
                        if (null != cdcConsumer) {
                            try {
                                if (!tables.isEmpty()) {
                                    hasHandelInit.set(true);
                                    //遍历monitorPath 所有子目录下的
                                    for (String table : tables) {
                                        final String tableSpace = monitorPath + "/" + table;
                                        File tableSpaceFile = new File(tableSpace);
                                        if (!tableSpaceFile.exists()) continue;
                                        Arrays.stream(Objects.requireNonNull(tableSpaceFile.listFiles()))
                                                .filter(file -> {
                                                    if (Objects.nonNull(file) && file.exists() && file.isFile()) {
                                                        String absolutePath = file.getAbsolutePath();
                                                        int indexOf = absolutePath.lastIndexOf('.');
                                                        String fileType = absolutePath.substring(indexOf + 1);
                                                        return file.isFile() && fileType.equalsIgnoreCase("csv") && isRecentCSV(file, System.currentTimeMillis() , cdcCacheTime);
                                                    } else {
                                                        return false;
                                                    }
                                                })
                                                .sorted(Comparator.comparing(File::getName))
                                                .forEach(file -> monitor(file, tableMap));
                                    }
                                }
                            } catch (Exception e) {
                                cdcConsumer.streamReadEnded();
                                context.getLog().warn("Start monitor file failed, msg: {}", e.getMessage());
                                throw new CoreException("Start monitor file failed, msg: {}", e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    cdcConsumer.streamReadEnded();
                    context.getLog().warn("Start monitor file failed, msg: {}", e.getMessage());
                    throw new CoreException("Start monitor file failed, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onStop(FileAlterationObserver observer) {
                super.onStop(observer);
                try {
                    if (!tables.isEmpty()) {
                        //遍历monitorPath 所有子目录下的
                        for (String table : tables) {
                            final String tableSpace = monitorPath + "/" + table;
                            File tableSpaceFile = new File(tableSpace);
                            if (!tableSpaceFile.exists()) continue;
                            Arrays.stream(Objects.requireNonNull(tableSpaceFile.listFiles()))
                                    .filter(file -> {
                                        if (Objects.nonNull(file) && file.exists() && file.isFile()) {
                                            String absolutePath = file.getAbsolutePath();
                                            int indexOf = absolutePath.lastIndexOf('.');
                                            String fileType = absolutePath.substring(indexOf + 1);
                                            return file.isFile() && fileType.equalsIgnoreCase("csv");
                                        } else {
                                            return false;
                                        }
                                    })
                                    .sorted(Comparator.comparing(File::getName))
                                    .forEach(file -> monitor(file, tableMap));
                        }
                    }
                } catch (Exception e) {
                    context.getLog().warn("Failed exec stop once, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onFileChange(File file0) {
                try {
                    monitor(file0, tableMap);
                } catch (Exception e) {
                    context.getLog().error("Monitor file change failed, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onFileCreate(File file0) {
                try {
                    monitor(file0, tableMap);
                } catch (Exception e) {
                    context.getLog().error("Monitor file change failed, msg: {}", e.getMessage());
                }
            }

            private boolean monitor(File file, Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap) {
                lastHeartbeatTime = System.currentTimeMillis();
                boolean isThisFile = false;
                String absolutePath = file.getAbsolutePath();
                int indexOf = absolutePath.lastIndexOf('.');
                String fileType = absolutePath.substring(indexOf + 1);
                if (file.isFile() && fileType.equalsIgnoreCase("csv")) {
                    //root.getContext().getLog().warn("File is modify: {}", file.getAbsolutePath());
                    String csvFileName = file.getName();
                    String[] split = csvFileName.split("\\.");
                    //root.getContext().getLog().info("An cdc event has be monitored file: {}, {}", absolutePath, split);
                    if (split.length < 3) {
                        throw new CoreException("Can not get table name from cav name, csv name is: {}", csvFileName);
                    }
                    String tableName = split[2];
                    //切换csv文件时删除之前的csv文件
                    //deleteTempCSV(tableName, absolutePath);
                    CdcPosition position = analyseCsvFile.getPosition();
                    if (null != tableName && tables.contains(tableName)) {
                        isThisFile = true;
                        //final TapTable tapTable = tableMap.get(tableName);
                        if (tableMap.isEmpty()) {
                            tableMap.putAll(getTableFromConfig(tables));
                        }
                        LinkedHashMap<String, TableTypeEntity> tapTable = tableMap.get(tableName);
                        if (null == tapTable) {
                            tableMap.putAll(getTableFromConfig(tables));
                            tapTable = tableMap.get(tableName);
                        }
                        if (null == tapTable || tapTable.isEmpty()) {
                            root.getContext().getLog().warn("Can not get table info from schemas.yaml of table {}", tableName);
                            return isThisFile;
                        }

                        final List<TapEvent>[] events = new List[]{new ArrayList<>()};
                        CdcPosition.PositionOffset positionOffset = position.get(tableName);
                        if (null == positionOffset) {
                            positionOffset = new CdcPosition.PositionOffset();
                            position.add(tableName, positionOffset);
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
                        ReadCSVOfBigFile readCSVOfBigFile = new ReadCSVOfBigFile();
                        readCSVOfBigFile.setLog(root.getContext().getLog());
                        try {
                            analyseCsvFile.analyse(file.getAbsolutePath(), tapTable, (compile, firstIndex, lastIndex) -> {
                                int csvFileLines = compile.size();
                                LinkedHashMap<String, TableTypeEntity> tableItem = tapTableAto.get();
                                CdcPosition.CSVOffset offset = csvOffsetAto.get();
                                int lineItem = offset.getLine();
                                if (lineItem <= lastIndex) {
                                    for (int index = 0; index < csvFileLines; index++) {
                                        if ((firstIndex + index) < lineItem) {
                                            continue;
                                        }
                                        TapEvent recordEvent;
                                        try {
                                            recordEvent = analyseRecord.analyse(compile.get(index), tableItem, tableName, config, nodeConfig);
                                        } catch (Exception e) {
                                            root.getContext().getLog().warn("An cdc event failed to accept in {} of {}, error csv format, csv line: {}, msg: {}", tableName, absolutePath, compile.get(index), e.getMessage());
                                            continue;
                                        }
                                        if (null != recordEvent) {
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
                            }).compile(readCSVOfBigFile, csvOffset.getLine());
                        } finally {
                            if (!events[0].isEmpty()) {
                                csvOffset.setOver(true);
                                cdcConsumer.accept(events[0], position);
                                events[0] = new ArrayList<>();
                            }
                        }
                    }
                }
                return isThisFile;
            }

            private void deleteCSVWithConfigTime(File file, Integer cdcCacheTime) {
                if (!isRecentCSV(file, System.currentTimeMillis(), cdcCacheTime)) {
                    try {
                        monitor(file, tableMap);
                        FileUtils.delete(file);
                    } catch (Exception ignore) {
                        root.getContext().getLog().warn("Can not to delete cdc cache file in {} of table name: {}", file.getAbsolutePath());
                    }
                }
            }

            private boolean isRecentCSV(File file, long compileTime, Integer cdcCacheTime) {
                String absolutePath = file.getAbsolutePath();
                int indexOf = absolutePath.lastIndexOf('.');
                String fileType = absolutePath.substring(indexOf + 1);
                if (file.isFile() && fileType.equalsIgnoreCase("csv")) {
                    long lastModify = file.lastModified();
                    //long now = System.currentTimeMillis();
                    // .    .    .
                    if (compileTime <= (cdcCacheTime + lastModify)) {
                        root.getContext().getLog().debug("File: {}, last modify time: {}, now: {}, cdc cache time: {}, need delete: true", file.getName(), lastModify, cdcCacheTime, cdcCacheTime);
                        return true;
                    }
                }
                return false;
            }
        });

        try {
            fileMonitor.start();
        } catch (Exception e) {
            fileMonitor.stop();
            throw new CoreException("Can not monitor cdc for sybase, msg: {}", e.getMessage());
        }
        return this.root;
    }

    private Map<String, LinkedHashMap<String, TableTypeEntity>> getTableFromConfig(List<String> tableId) {
        Map<String, LinkedHashMap<String, TableTypeEntity>> table = new LinkedHashMap<>();
        if (null == tableId || tableId.isEmpty()) return table;
        final ConnectionConfig config = new ConnectionConfig(root.getContext());
        final String username = config.getUsername();
        final String database = config.getDatabase();
        final String schema = config.getSchema();
        try {
            YamlUtil schemas = new YamlUtil(schemaConfigPath);
            List<Map<String, Object>> schemaList = (List<Map<String, Object>>) schemas.get("schemas");
            for (Map<String, Object> objectMap : schemaList) {
                Object catalog = objectMap.get("catalog");
                Object schemaItem = objectMap.get("schema");
                if (null != catalog && null != schemaItem && catalog.equals(database) && schemaItem.equals(schema)) {
                    Object tables = objectMap.get("tables");
                    if (!(tables instanceof Collection)) continue;
                    ((Collection<Map<String, Object>>) tables).stream()
                            .filter(map -> Objects.nonNull(map) && tableId.contains(String.valueOf(map.get("name"))))
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
        return table;
    }

}
