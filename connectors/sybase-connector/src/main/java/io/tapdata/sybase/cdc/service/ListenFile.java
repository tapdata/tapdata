package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseRecord;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseTapEventFromCsvString;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.watch.FileListener;
import io.tapdata.sybase.cdc.dto.watch.FileMonitor;
import io.tapdata.sybase.cdc.dto.watch.StopLock;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;
import io.tapdata.sybase.util.YamlUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author GavinXiao
 * @description LinstenFile create by Gavin
 * @create 2023/7/13 11:32
 **/
public class ListenFile implements CdcStep<CdcRoot> {
    public static final String TAG = ListenFile.class.getSimpleName();
    CdcRoot root;
    String monitorPath; ///sybase-poc/config/sybase2csv/csv/testdb/tester
    StopLock lock;
    AnalyseCsvFile analyseCsvFile;
    String monitorFileName;
    List<String> tables;
    AnalyseRecord<List<String>, TapRecordEvent> analyseRecord;
    StreamReadConsumer cdcConsumer;
    int batchSize;
    final String schemaConfigPath;
    String currentFileName;
    final ConnectionConfig config;
    final NodeConfig nodeConfig;

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
    }

    FileMonitor fileMonitor;

    public ListenFile monitor(FileMonitor monitor) {
        this.fileMonitor = monitor;
        this.cdcConsumer = monitor.getCdcConsumer();
        return this;
    }
    //super.onFileChange(file);

    //                if (monitorFileName.equals(name)) {
//                    //读取object_metadata.yaml
//                    /**
//                     * ctrl-chars:
//                     *   delimiter: ','
//                     *   escape: '"'
//                     *   line-end: '
//                     *
//                     *     '
//                     *   null-string: 'NULL'
//                     *   quote: '"'
//                     * file-row-count:
//                     * - file-name: testdb.tester.car_claim.part_0.csv
//                     *   row-count: 3
//                     * - file-name: testdb.tester.car_claim.part_1.csv
//                     *   row-count: 1
//                     * version: 0
//                     * */
//                    YamlUtil yamlUtil = new YamlUtil(file.getAbsolutePath());
//                    //获取file-row-count 值，其中包含了cdc产生的csv文件名以及文件记录的行数
//                    List<Map<String, Object>> cdcFilterCsvFileConfigs = (List<Map<String, Object>>)yamlUtil.get("file-row-count");
//                    CdcPosition position = analyseCsvFile.getPosition();
//
//                    String[] split = name.split("\\.");
//                    if (split.length < 3) {
//                        throw new CoreException("Can not get table name from cav name, csv name is: " + name);
//                    }
//                    String tableName = split[2];
//
//                    if (null != tableName && tables.contains(tableName)) {
//                        final TapTable tapTable = tableMap.get(tableName);
//                        CdcPosition.PositionOffset positionOffset = position.get(tableName);
//                        String fileName = positionOffset.getFileName();
//                        int line = positionOffset.getLine();
//                        boolean over = positionOffset.isOver();
//
//                        List<List<String>> compile = analyseCsvFile.analyse(file.getAbsolutePath()).compile();
//                        for (int index = 0; index < compile.size(); index++) {
//                            TapRecordEvent recordEvent = analyseRecord.analyse(compile.get(index), tapTable);
//                        }
//                    }
//                }
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
        final Map<String, LinkedHashMap<String, String>> tableMap = getTableFromConfig(root.getCdcTables());
        fileMonitor.monitor(monitorPath, new FileListener() {
            @Override
            public void onStart(FileAlterationObserver observer) {
                if (null == cdcConsumer) return;
                try {
                    super.onStart(observer);
                    //遍历monitorPath 所有子目录下的
                    for (String table : tables) {
                        final String tableSpace = monitorPath + "/" + table;
                        File tableSpaceFile = new File(tableSpace);
                        if (!tableSpaceFile.exists()) continue;
                        File[] files = tableSpaceFile.listFiles();
                        if (null != files && files.length > 0) {
                            for (File file : files) {
                                monitor(file);
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
            }

            @Override
            public void onFileChange(File file) {
                try {
                    monitor(file);
                } catch (Exception e) {
                    context.getLog().error("Monitor file change failed, msg: {}", e.getMessage());
                }
            }

            @Override
            public void onFileCreate(File file) {
                try {
                    monitor(file);
                } catch (Exception e) {
                    context.getLog().error("Monitor file create failed, msg: {}", e.getMessage());
                }
            }

            private boolean monitor(File file) {
                boolean isThisFile = false;
                String absolutePath = file.getAbsolutePath();
                int indexOf = absolutePath.lastIndexOf('.');
                String fileType = absolutePath.substring(indexOf + 1);
                if (file.isFile() && (fileType.equals("csv") || fileType.equals("CSV"))) {
                    String csvFileName = file.getName();
                    String[] split = csvFileName.split("\\.");
                    if (split.length < 3) {
                        throw new CoreException("Can not get table name from cav name, csv name is: {}", csvFileName);
                    }

                    //切换csv文件时删除之前的csv文件
                    if (null == currentFileName) {
                        currentFileName = absolutePath;
                    } else {
                        if (!absolutePath.equals(currentFileName)) {
                            File historyFile = new File(currentFileName);
                            if (historyFile.exists() && historyFile.isFile()) {
                                try {
                                    FileUtils.delete(historyFile);
                                } catch (Exception e) {
                                    root.getContext().getLog().info("Can not to delete cdc cache file in {}", currentFileName);
                                }
                            }
                            currentFileName = absolutePath;
                        }
                    }

                    String tableName = split[2];
                    CdcPosition position = analyseCsvFile.getPosition();
                    if (null != tableName && tables.contains(tableName)) {
                        isThisFile = true;
                        //final TapTable tapTable = tableMap.get(tableName);
                        if (tableMap.isEmpty()) {
                            tableMap.putAll(getTableFromConfig(tables));
                        }
                        LinkedHashMap<String, String> tapTable = tableMap.get(tableName);
                        if (null == tapTable) {
                            tableMap.putAll(getTableFromConfig(tables));
                            tapTable = tableMap.get(tableName);
                        }
                        if (null == tapTable || tapTable.isEmpty()) return isThisFile;
                        CdcPosition.PositionOffset positionOffset = position.get(tableName);
                        if (null == positionOffset) {
                            positionOffset = new CdcPosition.PositionOffset();
                            position.add(tableName, positionOffset);
                        }
                        CdcPosition.CSVOffset csvOffset = positionOffset.csvOffset(csvFileName);
                        int line = 0;
                        boolean over = false;
                        if (null != csvOffset) {
                            line = csvOffset.getLine();
                            over = csvOffset.isOver();
                        } else {
                            csvOffset = new CdcPosition.CSVOffset();
                            csvOffset.setOver(false);
                            csvOffset.setLine(0);
                            positionOffset.csvOffset(csvFileName, csvOffset);
                        }
                        if (over) {
                            csvOffset.setOver(false);
                        }

                        List<TapEvent> events = new ArrayList<>();
                        List<List<String>> compile = analyseCsvFile.analyse(file.getAbsolutePath()).compile();
                        int csvFileLines = compile.size();
                        for (int index = line; index < csvFileLines; index++) {
                            TapEvent recordEvent = analyseRecord.analyse(compile.get(index), tapTable, tableName, config, nodeConfig);
                            if (null != recordEvent) {
                                events.add(recordEvent);
                                if (events.size() == batchSize) {
                                    csvOffset.setOver(false);
                                    csvOffset.setLine(index + 1);
                                    cdcConsumer.accept(events, positionOffset);
                                    events = new ArrayList<>();
                                }
                            }
                        }
                        if (!events.isEmpty()) {
                            csvOffset.setOver(true);
                            csvOffset.setLine(csvFileLines - 1);
                            cdcConsumer.accept(events, positionOffset);
                        }
                    }
                }
                return isThisFile;
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

    private Map<String, LinkedHashMap<String, String>> getTableFromConfig(List<String> tableId) {
        Map<String, LinkedHashMap<String, String>> table = new LinkedHashMap<>();
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
                            .findFirst().ifPresent(tableInfo -> {
                        Object columns = tableInfo.get("columns");
                        if (columns instanceof Collection) {
                            String tableName = String.valueOf(tableInfo.get("name"));
                            Collection<Map<String, Object>> columnsList = (Collection<Map<String, Object>>) columns;
                            LinkedHashMap<String, String> tableClo = new LinkedHashMap<>();
                            columnsList.stream().filter(Objects::nonNull).forEach(clo -> {
                                String name = String.valueOf(clo.get("name"));
                                String type = String.valueOf(clo.get("type"));
                                tableClo.put(name, type);
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
