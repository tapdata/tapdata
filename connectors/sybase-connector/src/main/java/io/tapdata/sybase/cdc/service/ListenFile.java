package io.tapdata.sybase.cdc.service;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
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
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author GavinXiao
 * @description LinstenFile create by Gavin
 * @create 2023/7/13 11:32
 **/
public class ListenFile implements CdcStep<CdcRoot> {
    CdcRoot root;
    String monitorPath; ///sybase-poc/config/sybase2csv/csv/testdb/tester
    StopLock lock;
    AnalyseCsvFile analyseCsvFile;
    String monitorFileName;
    List<String> tables;
    AnalyseRecord<List<String>, TapRecordEvent> analyseRecord;
    StreamReadConsumer cdcConsumer;
    int batchSize;

    protected ListenFile(CdcRoot root,
                         String monitorPath,
                         List<String> tables,
                         String monitorFileName,
                         AnalyseCsvFile analyseCsvFile,
                         StopLock lock,
                         int batchSize,
                         StreamReadConsumer consumer) {
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
        this.cdcConsumer = consumer;
        this.batchSize = batchSize;
    }

    FileMonitor fileMonitor;

    public ListenFile monitor(FileMonitor monitor) {
        this.fileMonitor = monitor;
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
        final KVReadOnlyMap<TapTable> tableMap = context.getTableMap();
        fileMonitor.monitor(monitorPath, new FileListener() {
            @Override
            public void onStart(FileAlterationObserver observer) {
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
            }

            @Override
            public void onStop(FileAlterationObserver observer) {
                super.onStop(observer);
            }

            @Override
            public void onFileChange(File file) {
                monitor(file);
            }

            @Override
            public void onFileCreate(File file) {
                monitor(file);
            }

            private void monitor(File file) {
                String absolutePath = file.getAbsolutePath();
                int indexOf = absolutePath.lastIndexOf('.');
                String fileType = absolutePath.substring(indexOf + 1);
                if (file.isFile() && (fileType.equals("csv") || fileType.equals("CSV"))) {
                    String csvFileName = file.getName();
                    String[] split = csvFileName.split("\\.");
                    if (split.length < 3) {
                        throw new CoreException("Can not get table name from cav name, csv name is: " + csvFileName);
                    }
                    String tableName = split[2];
                    CdcPosition position = analyseCsvFile.getPosition();
                    if (null != tableName && tables.contains(tableName)) {
                        final TapTable tapTable = tableMap.get(tableName);
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
                            TapEvent recordEvent = analyseRecord.analyse(compile.get(index), tapTable);
                            if (null != recordEvent) {
                                events.add(recordEvent);
                                if (events.size() == batchSize) {
                                    csvOffset.setOver(false);
                                    csvOffset.setLine(index);
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
            }
        });
        try {
            fileMonitor.start();
        } catch (Exception e) {
            throw new CoreException("Can not monitor cdc for sybase, msg: " + e.getMessage());
        }
        return this.root;
    }
}
