package io.tapdata.common;

import com.amazonaws.transform.MapEntry;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.event.TapEvent;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class FileConnector extends ConnectorBase {

    protected FileConfig fileConfig;
    protected TapFileStorage storage;

    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig.load(connectorContext.getConnectionConfig());
        fileConfig.load(connectorContext.getNodeConfig());
        String clazz = FileProtocolEnum.fromValue(fileConfig.getProtocol()).getStorage();
        storage = new TapFileStorageBuilder()
                .withClassLoader(Class.forName(clazz).getClassLoader())
                .withParams(connectorContext.getConnectionConfig())
                .withStorageClassName(clazz)
                .build();
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
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
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try (
                FileTest fileTest = new FileTest(connectionContext.getConnectionConfig())
        ) {
            connectionOptions.connectionString(fileTest.getConnectionString());
            TestItem testConnect = fileTest.testConnect();
            consumer.accept(testConnect);
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
        Map<String, Long> allLastModified = fileOffset.getAllLastModified();
        while (isAlive()) {
            Map<String, Long> newLastModified = getFilteredFiles().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getLastModified()));
            AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(new ArrayList<>());
            String stopPath = fileOffset.getPath();
            if (EmptyKit.isNotBlank(stopPath) && newLastModified.get(stopPath) > allLastModified.get(stopPath)) {
                readOneFile(fileOffset, tapTable, recordSize, consumer, tapEvents);
                fileOffset.getAllLastModified().put(stopPath, newLastModified.get(stopPath));
                consumer.accept(Collections.emptyList(), fileOffset);
            }
            Iterator<Map.Entry<String, Long>> iterator = newLastModified.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator();
            while (isAlive() && iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                //new file or lastModified has changed
                if (!allLastModified.containsKey(entry.getKey()) || allLastModified.get(entry.getKey()) < entry.getValue()) {
                    fileOffset.setPath(entry.getKey());
                    makeFileOffset(fileOffset);
                    readOneFile(fileOffset, tapTable, recordSize, consumer, tapEvents);
                    fileOffset.getAllLastModified().put(entry.getKey(), entry.getValue());
                    consumer.accept(Collections.emptyList(), fileOffset);
                }
            }
            int sleep = 60;
            while (isAlive() && (sleep-- > 0)) {
                TapSimplify.sleep(1000);
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
        Map<String, TapFile> fileMap = getFilteredFiles();
        FileOffset fileOffset = new FileOffset();
        fileOffset.setAllLastModified(fileMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getLastModified())));
        return fileOffset;
    }

}
