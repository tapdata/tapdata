package io.tapdata.common;

import com.amazonaws.transform.MapEntry;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.util.MatchUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class FileConnector extends ConnectorBase {

    protected FileConfig fileConfig;
    protected TapFileStorage storage;
    protected AbstractFileRecordWriter fileRecordWriter;
    protected ExecutorService executorService;
    protected String firstConnectorId;
    private static final String TAG = FileConnector.class.getSimpleName();

    protected void initConnection(TapConnectionContext connectionContext) throws Exception {
        isConnectorStarted(connectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = connectionContext.getId();
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        fileConfig.load(connectionContext.getConnectionConfig());
        fileConfig.load(connectionContext.getNodeConfig());
        String clazz = FileProtocolEnum.fromValue(fileConfig.getProtocol()).getStorage();
        storage = new TapFileStorageBuilder()
                .withClassLoader(Class.forName(clazz).getClassLoader())
                .withParams(connectionContext.getConnectionConfig())
                .withStorageClassName(clazz)
                .build();
        if (EmptyKit.isNotBlank(fileConfig.getWriteFilePath()) && !storage.supportAppendData()) {
            initMergeCacheFilesThread();
        }
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (EmptyKit.isNotNull(fileRecordWriter)) {
            if (!storage.supportAppendData()) {
                fileRecordWriter.mergeCacheFiles();
            }
            fileRecordWriter.releaseResource();
        }
        storage.destroy();
    }

    protected ConcurrentMap<String, TapFile> getFilteredFiles() throws Exception {
        Set<TapFile> files = new HashSet<>();
        for (String path : fileConfig.getFilePathSet()) {
            storage.getFilesInDirectory(path, fileConfig.getIncludeRegs(), fileConfig.getExcludeRegs(), fileConfig.getRecursive(), 10, files::addAll);
        }
        return files.stream().collect(Collectors.toConcurrentMap(TapFile::getPath, Function.identity()));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try (
                FileTest fileTest = new FileTest(connectionContext.getConnectionConfig())
        ) {
            connectionOptions.connectionString(fileTest.getConnectionString());
            TestItem testConnect = fileTest.testConnect();
            if (TestItem.RESULT_SUCCESSFULLY == testConnect.getResult()) {
                consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
            }
            consumer.accept(testConnect);
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        //as file-connector this api has no meanings
        return 1;
    }

    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        return 0;
    }

    protected void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        Map<String, TapFile> fileMap = getFilteredFiles();
        if (EmptyKit.isEmpty(fileMap)) {
            return;
        }
        FileOffset fileOffset;
        //beginning
        if (null == offsetState) {
            fileOffset = new FileOffset();
            fileOffset.setPath(fileMap.entrySet().stream().min(Map.Entry.comparingByKey()).orElseGet(MapEntry::new).getKey());
            makeFileOffset(fileOffset);
        }
        //with offset
        else {
            fileOffset = (FileOffset) offsetState;
        }
        AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(new ArrayList<>());
        readOneFile(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        fileMap.entrySet().removeIf(v -> v.getValue().getPath().compareTo(fileOffset.getPath()) <= 0);
        Iterator<Map.Entry<String, TapFile>> iterator = fileMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator();
        while (isAlive() && iterator.hasNext()) {
            fileOffset.setPath(iterator.next().getValue().getPath());
            makeFileOffset(fileOffset);
            eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
            tapEvents.set(new ArrayList<>());
            readOneFile(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        }
        if (EmptyKit.isNotEmpty(tapEvents.get())) {
            fileOffset.setDataLine(fileOffset.getDataLine() + tapEvents.get().size());
            eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
        }
    }

    protected void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        TapTable tapTable = nodeContext.getTableMap().get(tableList.get(0));
        FileOffset fileOffset = (FileOffset) offsetState;
        Map<String, TapFile> allFiles = fileOffset.getAllFiles();
        Map<String, TapFile> tempFiles = allFiles.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, TapFile> needReadFiles = new HashMap<>();
        while (isAlive()) {
            Map<String, TapFile> newFiles = getFilteredFiles();
            AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(new ArrayList<>());
            tempFiles.entrySet().stream().filter(v -> !newFiles.containsKey(v.getKey())).forEach(v ->
                    TapLogger.warn(TAG, String.format("%s has been deleted, but this can change nothing", v.getKey())));
            newFiles.entrySet().stream().filter(v -> !tempFiles.containsKey(v.getKey())).forEach(v ->
                    TapLogger.info(TAG, String.format("%s has been found, it will take effect one minutes later", v.getKey())));
            Map<String, TapFile> changedFiles = newFiles.entrySet().stream().filter(v -> !tempFiles.containsKey(v.getKey())
                            || v.getValue().getLastModified() > tempFiles.get(v.getKey()).getLastModified()
                            || !Objects.equals(v.getValue().getLength(), tempFiles.get(v.getKey()).getLength()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Iterator<Map.Entry<String, TapFile>> iterator = needReadFiles.entrySet().stream()
                    .filter(v -> !changedFiles.containsKey(v.getKey())).sorted(Map.Entry.comparingByKey()).iterator();
            while (isAlive() && iterator.hasNext()) {
                Map.Entry<String, TapFile> entry = iterator.next();
                fileOffset.setPath(entry.getKey());
                makeFileOffset(fileOffset);
                consumer.accept(tapEvents.get(), fileOffset);
                tapEvents.set(new ArrayList<>());
                readOneFile(fileOffset, tapTable, recordSize, consumer, tapEvents);
                allFiles.put(entry.getKey(), entry.getValue());
            }
            if (EmptyKit.isNotEmpty(tapEvents.get())) {
                fileOffset.setDataLine(fileOffset.getDataLine() + tapEvents.get().size());
                consumer.accept(tapEvents.get(), fileOffset);
            }
            needReadFiles.clear();
            needReadFiles.putAll(changedFiles);
            tempFiles.putAll(newFiles);
            int sleep = 60;
            try {
                while (isAlive() && (sleep-- > 0)) {
                    TapSimplify.sleep(1000);
                }
            } catch (Exception ignore) {
            }
        }
    }

    protected void makeFileOffset(FileOffset fileOffset) {
        fileOffset.setDataLine(0);
    }

    protected abstract void readOneFile(FileOffset fileOffset,
                                        TapTable tapTable,
                                        int eventBatchSize,
                                        BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                                        AtomicReference<List<TapEvent>> tapEvents) throws Exception;

    protected Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Exception {
        FileOffset fileOffset = new FileOffset();
        fileOffset.setAllFiles(getFilteredFiles());
        return fileOffset;
    }

    protected void makeTapTable(TapTable tapTable, Map<String, Object> sample, boolean isJustString) {
        if (isJustString) {
            for (Map.Entry<String, Object> objectEntry : sample.entrySet()) {
                TapField field = new TapField();
                field.name(objectEntry.getKey());
                if (EmptyKit.isNotEmpty((String) objectEntry.getValue()) && ((String) objectEntry.getValue()).length() > 200) {
                    field.dataType("TEXT");
                } else {
                    field.dataType("STRING");
                }
                tapTable.add(field);
            }
        } else {
            for (Map.Entry<String, Object> objectEntry : sample.entrySet()) {
                TapField field = new TapField();
                field.name(objectEntry.getKey());
                String value = (String) objectEntry.getValue();
                if (EmptyKit.isEmpty(value)) {
                    field.dataType("STRING");
                } else if (MatchUtil.matchBoolean(value)) {
                    field.dataType("BOOLEAN");
                } else if (MatchUtil.matchInteger(value)) {
                    field.dataType("INTEGER");
                } else if (MatchUtil.matchNumber(value)) {
                    field.dataType("NUMBER");
                } else if (MatchUtil.matchDateTime(value)) {
                    field.dataType("DATETIME");
                } else if (value.length() > 200) {
                    field.dataType("TEXT");
                } else {
                    field.dataType("STRING");
                }
                tapTable.add(field);
            }
        }
    }

    protected void initMergeCacheFilesThread() {
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(() -> {
            int count = 0;
            while (isAlive()) {
                if (EmptyKit.isNotNull(fileRecordWriter)) {
                    count++;
                }
                TapSimplify.sleep(1000 * 60);
                if (count >= 5) {
                    try {
                        fileRecordWriter.mergeCacheFiles();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    count = 0;
                }
            }
        });
    }

}
