package io.tapdata.sybase.cdc.service;

import cn.hutool.core.text.csv.CsvReader;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.SybaseConnector;
import io.tapdata.sybase.cdc.CdcRoot;
import io.tapdata.sybase.cdc.CdcStep;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseRecord;
import io.tapdata.sybase.cdc.dto.analyse.AnalyseTapEventFromCsvString;
import io.tapdata.sybase.cdc.dto.analyse.CsvAnalyseFilter;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSV;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVOfBigFile;
import io.tapdata.sybase.cdc.dto.analyse.csv.ReadCSVQuickly;
import io.tapdata.sybase.cdc.dto.analyse.filter.ReadFilter;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.cdc.dto.start.SybaseFilterConfig;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final String monitorPath; ///${host:port:database}/sybase-poc/config/csv
    private final StopLock lock;
    private final AnalyseCsvFile analyseCsvFile;
    private final String monitorFileName;
    private Map<String, Map<String, List<String>>> tables; //all cdc table, <databaseName, <schemaName, tableName>>
    private final AnalyseRecord<String[], TapRecordEvent> analyseRecord; //the stage to analy csv line
    private final int batchSize;
    private final ConnectionConfig config;
    private final NodeConfig nodeConfig;
    private StreamReadConsumer cdcConsumer;
    private FileMonitor fileMonitor; //a process to monitor csv files, change or create
    private final ReadCSVOfBigFile readCSVOfBigFile = new ReadCSVOfBigFile();
    private final String schemaConfigPath; //a yaml file path name is schema.yaml which can get the cdc tables
    private final Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap = new HashMap<>(); //all cdc table from schema.yaml to anlyse csv line to tap event

    private Queue<String> monitorFilePathQueues;
    private ScheduledFuture<?> futureCheckFile;
    private ScheduledFuture<?> futureReadFile;
    private final ScheduledExecutorService scheduledExecutorServiceCheckFile = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService scheduledExecutorService;
    private final Object readFileLock = new Object();
    Map<String, Set<String>> blockFieldsMap;
    Map<String, TapTable> tapTableMap = new HashMap<>();

    private final ReadFilter readFilter;
    
    private final int readcsvDelay;

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
        root.getContext().getLog().info("init with monitor table: {}", tables);
        analyseRecord = new AnalyseTapEventFromCsvString();
        this.schemaConfigPath = root.getSybasePocPath() + ConfigPaths.SCHEMA_CONFIG_PATH;
        this.config = root.getConnectionConfig();
        this.nodeConfig = root.getNodeConfig();
        readCSVOfBigFile.setLog(root.getContext().getLog());

        monitorFilePathQueues = new ConcurrentLinkedQueue<>();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        if (ReadFilter.LOG_CDC_QUERY_READ_SOURCE == nodeConfig.getLogCdcQuery()) {
            this.batchSize = batchSize < 200 || batchSize > 1000 ? 1000 : batchSize;
            this.readcsvDelay = 3;
            this.readCSVOfBigFile.setCdcBatchSize(ReadCSV.MAX_LINE_EVERY_CSV_FILE);
            blockFieldsMap = Optional.ofNullable(root.getExistsBlockFieldsMap()).orElse(ConnectorUtil.tableBlockFieldsFromFilterYaml(root.getSybasePocPath(), root.getContext()));
            int times = 3;
            while (root.getIsAlive().test(null)){
                try {
                    getTapTableMap();
                    break;
                } catch (Throwable e){
                    if (times > 0) {
                        times--;
                        try {
                            Thread.sleep(500);
                        } catch (Exception ignore){}
                    } else {
                        throw new CoreException("Can not get tap table from context when init cdc mointor, msg: {}", e.getMessage());
                    }
                }
            }
        } else {
            this.readcsvDelay = 1;
            this.readCSVOfBigFile.setCdcBatchSize(ReadCSV.CDC_BATCH_SIZE);
            this.batchSize = batchSize < 200 || batchSize > 500 ? 200 : batchSize;
            blockFieldsMap = new HashMap<>();
        }
        readFilter = ReadFilter.stage(Optional.ofNullable(nodeConfig.getLogCdcQuery()).orElse(ReadFilter.LOG_CDC_QUERY_READ_LOG), root);
    }

    private void getTapTableMap() throws Throwable {
        KVReadOnlyMap<TapTable> tableMap = root.getContext().getTableMap();
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        while (iterator.hasNext()) {
            Entry<TapTable> next = iterator.next();
            tapTableMap.put(next.getKey(), next.getValue());
        }
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
        try {
            tableMap.putAll(getTableFromConfig(root.getCdcTables()));
        } catch (Exception e) {
            this.root.getContext().getLog().debug("Can not get Table from config before monitor start, msg: {}", e.getMessage());
        }
        AtomicBoolean hasHandelInit = new AtomicBoolean(false);

        final Integer cdcCacheTime = Optional.ofNullable(nodeConfig.getCdcCacheTime()).orElse(ReadCSV.DEFAULT_CACHE_TIME_OF_CSV_FILE) * 60000;
        FileListenerImpl listener = new FileListenerImpl(context, hasHandelInit, cdcCacheTime) ;
        //fileMonitor.monitor(monitorPath, listener);
        try {
            if (Objects.nonNull(this.futureCheckFile)) {
                try {
                    this.futureCheckFile.cancel(true);
                } catch (Exception e1){ } finally {
                    this.futureCheckFile = null;
                }
            }
            if (Objects.nonNull(futureReadFile)) {
                try {
                    futureReadFile.cancel(true);
                } catch (Exception e1){ } finally {
                    futureReadFile = null;
                }
            }

            this.futureCheckFile = this.scheduledExecutorServiceCheckFile.scheduleWithFixedDelay(() -> listener.foreachYaml(false), 0, readcsvDelay, TimeUnit.SECONDS);
            this.futureReadFile = this.scheduledExecutorService.scheduleWithFixedDelay(() -> listener.readFile(), 2, readcsvDelay, TimeUnit.SECONDS);
            //fileMonitor.start();
        } catch (Throwable e) {
            onStop();
            if (e instanceof CoreException && SybaseConnector.CDC_PROCESS_FAIL_EXCEPTION_CODE == ((CoreException)e).getCode()) {
                throw (CoreException)e;
            }
            throw new CoreException("Can not monitor cdc for sybase, msg: {}", e.getMessage());
        }

        return this.root;
    }

    public void onStop() {
        try {
            Optional.ofNullable(fileMonitor).ifPresent(FileMonitor::stop);
        } catch (Exception e) {
            //root.getContext().getLog().debug("Cdc monitor stop fail, msg: {}", e.getMessage());
        }
        try {
            Optional.ofNullable(futureCheckFile).ifPresent(f -> f.cancel(true));
        } catch (Exception e) {
            root.getContext().getLog().debug("Cdc monitor stop fail, msg: {}", e.getMessage());
        } finally {
            futureCheckFile = null;
        }
        try {
            Optional.ofNullable(futureReadFile).ifPresent(f -> f.cancel(true));
        } catch (Exception e) {
            root.getContext().getLog().debug("Cdc monitor stop fail, msg: {}", e.getMessage());
        } finally {
            futureReadFile = null;
        }

    }

    public ListenFile monitor(FileMonitor monitor) {
        this.fileMonitor = monitor;
        this.cdcConsumer = monitor.getCdcConsumer();
        return this;
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
                        final String tableFullNameSuff = catalog + "." + schemaItem + ".";
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
                                            table.put(tableFullNameSuff + tableName, tableClo);
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
            root.getContext().getLog().info("Change monitor table to: {}", tables);
        }
    }

    class FileListenerImpl {
        final int cdcCacheTime;
        final AtomicBoolean hasHandelInit;
        final TapConnectorContext context;
        final Log log;

        FileListenerImpl(TapConnectorContext context, AtomicBoolean hasHandelInit, int cdcCacheTime) {
            super();
            if (null == context) {
                throw new CoreException("TapConnectorContext can not be empty");
            }
            this.cdcCacheTime = cdcCacheTime;
            this.hasHandelInit = hasHandelInit;
            this.context = context;
            this.log = context.getLog();
            if (null == this.log) {
                throw new CoreException("TapConnectorContext'log can not be empty");
            }
        }

        public void foreachTable(boolean needDeleteCsvFile) {
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
                        Integer csvIndex = Optional.ofNullable(root.getCsvFileModifyIndexByCsvFileName(fullTableName(database, schema, table))).orElse(0);
                        Arrays.stream(Objects.requireNonNull(tableSpaceFile.listFiles()))
                                .filter(file -> Objects.nonNull(file) && file.exists() && file.isFile() && isCSV(file) && csvIndex <= CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(file.getName()))
                                .sorted(Comparator.comparing(File::getName))
                                .forEach(file -> monitorCSVFileIfPresentDeleteWithConfigTime(file, cdcCacheTime, needDeleteCsvFile));
                    }
                }
            }
        }

        public boolean isCSV(File file) {
            if (Objects.nonNull(file) && file.exists() && file.isFile()) {
                String absolutePath = file.getAbsolutePath();
                int indexOf = absolutePath.lastIndexOf('.');
                String fileType = absolutePath.substring(indexOf + 1);
                return fileType.equalsIgnoreCase("csv");
            } else {
                return false;
            }
        }

        public boolean monitor(File file, Map<String, LinkedHashMap<String, TableTypeEntity>> tableMap) {
            boolean isThisFile = false;
            String absolutePath = file.getAbsolutePath();
            String csvFileName = file.getName();
            String[] split = csvFileName.split("\\.");
            if (split.length < 3) {
                throw new CoreException("Can not get table name from cav name, csv name is: {}", csvFileName);
            }
            final String database = split[0];
            final String schema = split[1];
            final String tableName = split[2];
            final String fullTableName = fullTableName(database, schema, tableName);

            CdcPosition position = analyseCsvFile.getPosition();
            long cdcStartTime = position.getCdcStartTime();
            if (file.lastModified() < cdcStartTime) {
                return false;
            }
            Map<String, List<String>> schemaTableMap = tables.get(database);
            if (null == schemaTableMap) {
                return false;
            }
            List<String> currentTables = schemaTableMap.get(schema);
            if (null == schemaTableMap.get(schema)) {
                return false;
            }
            if (null != tableName && currentTables.contains(tableName)) {
                isThisFile = true;
                if (tableMap.isEmpty()) {
                    try {
                        tableMap.putAll(getTableFromConfig(tables));
                    } catch (Exception e){
                        root.getContext().getLog().info("Can not get Table from config, msg: {}", e.getMessage());
                    }
                }
                final String tableFullName = database + "." + schema + "." + tableName;
                LinkedHashMap<String, TableTypeEntity> tapTable = tableMap.get(tableFullName);
                if (null == tapTable) {
                    try {
                        tableMap.putAll(getTableFromConfig(tables));
                    } catch (Exception e){
                        root.getContext().getLog().info("Can not get Table from config, msg: {}", e.getMessage());
                    }
                    tapTable = tableMap.get(tableFullName);
                }
                if (null == tapTable || tapTable.isEmpty()) {
                    root.getContext().getLog().warn("Can not get table info from schemas.yaml of table {}", tableName);
                    return false;
                }
                final List<TapEvent>[] events = new List[]{new ArrayList<>()};
                CdcPosition.PositionOffset positionOffset = position.get(database, schema, tableName);
                if (null == positionOffset) {
                    positionOffset = new CdcPosition.PositionOffset(absolutePath.replace(csvFileName, ""));
                    position.add(database, schema, tableName, positionOffset);
                }
                CdcPosition.CSVOffset csvOffset = positionOffset.csvOffset(csvFileName);
                if (null == csvOffset) {
                    csvOffset = new CdcPosition.CSVOffset();
                    csvOffset.setOver(false);
                    csvOffset.setLine(root.getCsvFileModifyLineByCsvFileName(csvFileName));
                    positionOffset.csvOffset(csvFileName, csvOffset);
                }
                if (csvOffset.getLine() >= ReadCSVQuickly.MAX_LINE_EVERY_CSV_FILE) {
                    return false;
                }
                AtomicReference<LinkedHashMap<String, TableTypeEntity>> tapTableAto = new AtomicReference<>(tapTable);
                AtomicReference<CdcPosition.CSVOffset> csvOffsetAto = new AtomicReference<>(csvOffset);
                final List<String> options = new ArrayList<>();
                options.add(database);
                options.add(schema);
                options.add(tableName);
                AtomicInteger count = new AtomicInteger(0);
                final Set<String> blockFields = blockFieldsMap.get(fullTableName);
                final TapTable tableInfo = tapTableMap.get(fullTableName);
                try {
                    analyseCsvFile.analyse(file.getAbsolutePath(), tapTable, (compile, firstIndex, lastIndex) -> {
                        LinkedHashMap<String, TableTypeEntity> tableItem = tapTableAto.get();
                        CdcPosition.CSVOffset offset = csvOffsetAto.get();
                        int lineItem = offset.getLine();
                        for (String[] list : compile) {
                            if (!root.getIsAlive().test(null)) break;
                            TapEvent recordEvent;
                            try {
                                recordEvent = analyseRecord.analyse(list, tableItem, tableName, config, nodeConfig, null);
                            } catch (Exception e) {
                                root.getContext().getLog().warn("An cdc event failed to accept in {} of {}, error csv format, csv line: {}, msg: {}", tableName, absolutePath, list, e.getMessage());
                                continue;
                            }
                            if (null != recordEvent) {
                                //共享挖掘不做忽略，引擎自动处理，事件发生的时间早于任务启动时间，当前事件需要被忽略
                                if (!position.isMultiTask() && lineItem <= 0 && ((TapRecordEvent) recordEvent).getReferenceTime() < position.getCdcStartTime()) {
                                    continue;
                                }
                                ((TapRecordEvent) recordEvent).setNamespaces(options);
                                events[0].add(recordEvent);
                                lineItem = offset.addAndGet();
                                count.addAndGet(1);
                                if (events[0].size() == batchSize) {
                                    offset.setOver(false);
                                    cdcConsumer.accept(readFilter.readFilter(events[0], tableInfo, blockFields, fullTableName), root.setCsvFileModifyIndexByCsvFileName(
                                            csvFileName,
                                            CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(csvFileName),
                                            lineItem));
                                    events[0] = new ArrayList<>();
                                }
                            }
                        }
                    }).compile(readCSVOfBigFile, csvOffset.getLine());
                } finally {
                    if (!events[0].isEmpty()) {
                        csvOffset.setOver(true);
                        cdcConsumer.accept(readFilter.readFilter(events[0], tableInfo, blockFields, fullTableName), root.setCsvFileModifyIndexByCsvFileName(
                                csvFileName,
                                CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(csvFileName),
                                csvOffset.getLine()));
                        events[0] = new ArrayList<>();
                    }
                    if (count.get() > 0) {
                        log.debug("File read once, read line: {}, file name: {}, nanoTime: {}", count.get(), file.getName(), System.nanoTime());
                    }
                }
            }
            return isThisFile;
        }

        public void monitorCSVFileIfPresentDeleteWithConfigTime(File file, Integer cdcCacheTime, boolean needClean) {
            //root.getContext().getLog().warn("start monitor file once, {}", file.getAbsolutePath());
            if (isRecentCSV(file, System.currentTimeMillis(), cdcCacheTime)) {
                //root.getContext().getLog().warn("monitor file once, {}", file.getAbsolutePath());
                monitor(file, tableMap);
            } else {
                if (needClean) {
                    try {
                        FileUtils.delete(file);
                    } catch (Exception ignore) {
                        root.getContext().getLog().info("Can not to delete cdc cache file in {} of table name: {}", file.getAbsolutePath());
                    }
                }
            }
        }

        public boolean isRecentCSV(File file, long compileTime, Integer cdcCacheTime) {
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

        public synchronized void readFile() {
            if (hasHandelInit.get()) {
                while (root.getIsAlive().test(null) && !monitorFilePathQueues.isEmpty()) {
                    File file = new File(monitorFilePathQueues.poll());
                    synchronized (readFileLock) {
                        monitor(file, tableMap);
                    }
                }
            }
        }

        /**
         * @deprecated
         * 释放断点信息，避免csv过多时内存不够，每个csv最多1000行，达到1000后不再需要保存它的信息, 文件同时清空
         * 超过缓存时间的文件需要被删除，一般10min
         * */
        public void releaseOffset() {
            CdcPosition position = analyseCsvFile.getPosition();
            if (null == position) return;
            Map<String, Map<String, Map<String, CdcPosition.PositionOffset>>> databaseOffset = position.getTableOffset();
            final long streamStartAndCacheTime = (position.getCdcStartTime() > 0 ?  position.getCdcStartTime() : System.currentTimeMillis()) - cdcCacheTime;
            if (null == databaseOffset || databaseOffset.isEmpty()) return;
            Set<Map.Entry<String, Map<String, Map<String, CdcPosition.PositionOffset>>>> entries = databaseOffset.entrySet();
            StringJoiner failStr = new StringJoiner(", ");
            StringJoiner unExistStr = new StringJoiner(", ");
            for (Map.Entry<String, Map<String, Map<String, CdcPosition.PositionOffset>>> entry : entries) {
                String database = entry.getKey();
                Map<String, Map<String, CdcPosition.PositionOffset>> schemaOffset = entry.getValue();
                if (null == schemaOffset || schemaOffset.isEmpty()) continue;
                Set<Map.Entry<String, Map<String, CdcPosition.PositionOffset>>> entrySet = schemaOffset.entrySet();
                for (Map.Entry<String, Map<String, CdcPosition.PositionOffset>> offsetOfCSV : entrySet) {
                    String schema = offsetOfCSV.getKey();
                    Map<String, CdcPosition.PositionOffset> tableOffset = offsetOfCSV.getValue();
                    Set<String> tables = tableOffset.keySet();
                    if (tables.isEmpty()) continue;
                    for (String table : tables) {
                        CdcPosition.PositionOffset csvOffsetMap = tableOffset.get(table);
                        if (null == csvOffsetMap) continue;
                        final String fileSuf = csvOffsetMap.getPathSuf();
                        Map<Integer, CdcPosition.CSVOffset> csvFile = csvOffsetMap.getCsvFile();
                        if (null == csvFile || csvFile.isEmpty()) continue;
                        List<Integer> fileNameList = new ArrayList<>(csvFile.keySet());
                        fileNameList.stream().filter(name -> {
                            if (null == name) return false;
                            CdcPosition.CSVOffset csvOffset = csvFile.get(name);
                            return null != csvOffset && csvOffset.getLine() >= ReadCSVQuickly.MAX_LINE_EVERY_CSV_FILE ;
                        }).forEach(fileIndex -> {
                            final String filePath = csvOffsetMap.parseFileName(database, schema, table, fileIndex);
                            File file = new File(filePath);
                            try {
                                if (file.exists() && file.isFile() && file.lastModified() < streamStartAndCacheTime) {
                                    ConnectorUtil.deleteFile(file, log);
                                    if (!file.exists()) {
                                        csvFile.remove(fileIndex);
                                        unExistStr.add(filePath);
                                    }
                                    //(new File(name)).createNewFile();
                                }
                            } catch (Exception e){
                                failStr.add(filePath + ", mag: " + e.getMessage());
                            }
                        });
                    }
                }
                context.getLog().debug("Files has 1000 lines which all has accepted in stream read, has delete offset of this file and remove this file forever, file list is: {}", unExistStr.toString());
                context.getLog().debug("Files has 1000 lines which all has accepted in stream read, fail to delete offset of this file and remove this file forever, file list with msg are: {}", failStr.toString());
            }
        }

        Map<String, Long> metadataYamlFileModifyTimeCache = new HashMap<>();
        public void foreachYaml(boolean needDeleteCsvFile) {
            if (tables.isEmpty()) return;
            //遍历monitorPath 所有子目录下的
            for (Map.Entry<String, Map<String, List<String>>> databaseEntry : tables.entrySet()) {
                if (!root.getIsAlive().test(null)) break;
                String database = databaseEntry.getKey();
                Map<String, List<String>> schemaMap = databaseEntry.getValue();
                if (null == database || null == schemaMap || schemaMap.isEmpty())  continue;
                final String tableSpace = monitorPath + "/" + database + "/";
                Set<Map.Entry<String, List<String>>> schemaEntry = schemaMap.entrySet();
                for (Map.Entry<String, List<String>> schemaMaps : schemaEntry) {
                    if (!root.getIsAlive().test(null)) break;
                    String schema = schemaMaps.getKey();
                    List<String> tablesMaps = schemaMaps.getValue();
                    if (null == schema || null == tablesMaps || tablesMaps.isEmpty()) continue;
                    final String tempDir = tableSpace + schema + "/";
                    for (String table : tablesMaps) {
                        if (!root.getIsAlive().test(null)) break;
                        if (null == table || "".equals(table.trim())) continue;
                        final String tempPath = tempDir + table + "/object_metadata.yaml";
                        File metadataYamlFile = new File(tempPath);
                        if (metadataYamlFile.exists() && metadataYamlFile.isFile() && isObjectMetadataYaml(metadataYamlFile)) {
//                            Long metadataYamlLastModify = metadataYamlFileModifyTimeCache.get(metadataYamlFile.getAbsolutePath());
//                            long lastModified = metadataYamlFile.lastModified();
//                            if (null == metadataYamlLastModify || metadataYamlLastModify < lastModified || (!hasHandelInit.get() && metadataYamlLastModify == lastModified )) {
//                            metadataYamlFileModifyTimeCache.put(metadataYamlFile.getAbsolutePath(), lastModified);
                            //选举一个最空闲的队列来处理当前表的全部新修改会新增的csv
                            readObjectMetadataYaml(metadataYamlFile);
//                            }
                        }
                    }
                }
            }
            if (!hasHandelInit.get()) {
                hasHandelInit.set(true);
            }
        }

        //Map<String, Long> tableModifyTimeCache = new HashMap<>();
        public void readObjectMetadataYaml(File file) {
            try {
                YamlUtil objectMetadataYaml = new YamlUtil(file.getAbsolutePath());
                List<Map<String, Object>> csvFileOffset = (List<Map<String, Object>>) objectMetadataYaml.get("file-row-count");
                csvFileOffset.stream().filter(Objects::nonNull).forEach(offset -> {
                    if (!root.getIsAlive().test(null)) return;
                    String csvFilePath = String.valueOf(offset.get("file-name"));
                    String absolutePath = file.getParentFile().getAbsolutePath();
                    File csvFile = new File(FilenameUtils.concat(absolutePath, csvFilePath));
                    //String databaseTable = databaseTable(csvFile);
                    //Long lastModifyTime = Optional.ofNullable(tableModifyTimeCache.get(databaseTable)).orElse(0L);
                    if (null != csvFile && csvFile.exists() && csvFile.isFile() && isCSV(csvFile)) {
                        String csvFileName = csvFile.getName();
                        Integer fileIndex = root.getCsvFileModifyIndexByCsvFileName(csvFileName);
                        Integer fileIndexFromCsvName = CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(csvFileName);
                        //上次读到第fileIndex个csv文件，当前是第fileIndexFromCsvName个csv文件，
                        if (null == fileIndex || fileIndex <= fileIndexFromCsvName){
                            log.debug("Yes, it's file: {}, file history index: {}, current index: {}", csvFileName, fileIndex, fileIndexFromCsvName);
                                //|| ( (!hasHandelInit.get() || lastModifyTime <  csvFile.lastModified() ) && fileIndex == fileIndexFromCsvName) ) {
                            monitorFilePathQueues.add(csvFile.getAbsolutePath());
                        }
                    }
                });
            } catch (Exception e) {
                log.info("Unable read {}, msg: {}", file.getAbsolutePath(), e.getMessage());
            }
        }

        public boolean isObjectMetadataYaml(File file) {
            if (Objects.nonNull(file) && file.exists() && file.isFile()) {
                return "object_metadata.yaml".equals(file.getName());
            } else {
                return false;
            }
        }
    }


    public String databaseTable(File csvFile) {
        return databaseTable(csvFile.getName());
    }
    public static String databaseTable(String csvFileName) {
        String[] split = csvFileName.split("\\.");
        if (split.length < 3) {
            throw new CoreException("Can not get table name from cav name, csv name is: {}", csvFileName);
        }
        final String database = split[0];
        final String schema = split[1];
        final String tableName = split[2];
        return fullTableName(database, schema, tableName);
    }

    public static String fullTableName(String database, String schema, String tableName) {
        return database + "." + schema + "." + tableName;
    }
}
