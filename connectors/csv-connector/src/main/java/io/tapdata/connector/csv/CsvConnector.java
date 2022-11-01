package io.tapdata.connector.csv;

import com.amazonaws.transform.MapEntry;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.FileProtocolEnum;
import io.tapdata.common.FileTest;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.connector.csv.util.MatchUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.util.DateUtil;

import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@TapConnectorClass("spec_csv.json")
public class CsvConnector extends ConnectorBase {

    private CsvConfig csvConfig;
    private TapFileStorage storage;

    private void initConnection(TapConnectionContext connectorContext) throws Exception {
        csvConfig = (CsvConfig) new CsvConfig().load(connectorContext.getConnectionConfig());
        isConnectorStarted(connectorContext, tapConnectorContext -> csvConfig.load(tapConnectorContext.getNodeConfig()));
        String clazz = FileProtocolEnum.fromValue(csvConfig.getProtocol()).getStorage();
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

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        //as file-connector: nodeConfig is supported, so initConnection can be used
        initConnection(connectionContext);
        TapTable tapTable = table(csvConfig.getModelName());
        ConcurrentMap<String, TapFile> csvFileMap = getFilteredFiles();
        CsvSchema csvSchema = new CsvSchema(csvConfig, storage);
        Map<String, Object> sample;
        //csv has column header
        if (EmptyKit.isNotBlank(csvConfig.getHeader())) {
            sample = csvSchema.sampleFixedFileData(csvFileMap);
        } else //analyze every csv file
        {
            sample = csvSchema.sampleEveryFileData(csvFileMap);
        }
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from csv files error: no headers and contents!");
        }
        if (csvConfig.getJustString()) {
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
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }

    private ConcurrentMap<String, TapFile> getFilteredFiles() throws Exception {
        Set<TapFile> csvFiles = new HashSet<>();
        for (String path : csvConfig.getFilePathSet()) {
            storage.getFilesInDirectory(path, csvConfig.getIncludeRegs(), csvConfig.getExcludeRegs(), csvConfig.getRecursive(), 10, csvFiles::addAll);
        }
        return csvFiles.stream().collect(Collectors.toConcurrentMap(TapFile::getPath, Function.identity()));
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

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        return 0;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        Map<String, TapFile> fileMap = getFilteredFiles();
        if (EmptyKit.isEmpty(fileMap)) {
            return;
        }
        CsvOffset csvOffset;
        //beginning
        if (null == offsetState) {
            csvOffset = new CsvOffset(fileMap.entrySet().stream().findFirst().orElseGet(MapEntry::new).getKey(), csvConfig.getDataStartLine() + (csvConfig.getIncludeHeader() ? 1 : 0));
        }
        //with offset
        else {
            csvOffset = (CsvOffset) offsetState;
        }
        AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(new ArrayList<>());
        readOneFile(csvOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        fileMap.entrySet().removeIf(v -> v.getValue().getPath().compareTo(csvOffset.getPath()) <= 0);
        Iterator<Map.Entry<String, TapFile>> iterator = fileMap.entrySet().iterator();
        while (isAlive() && iterator.hasNext()) {
            csvOffset.setPath(iterator.next().getValue().getPath());
            csvOffset.setDataLine(csvConfig.getDataStartLine() + (csvConfig.getIncludeHeader() ? 1 : 0));
            readOneFile(csvOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        }
        if (EmptyKit.isNotEmpty(tapEvents.get())) {
            csvOffset.setDataLine(csvOffset.getDataLine() + tapEvents.get().size());
            eventsOffsetConsumer.accept(tapEvents.get(), csvOffset);
        }
    }

    private void readOneFile(CsvOffset csvOffset,
                             TapTable tapTable,
                             int eventBatchSize,
                             BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                             AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        Map<String, String> dataTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
        try (
                Reader reader = new InputStreamReader(storage.readFile(csvOffset.getPath()));
                CSVReader csvReader = new CSVReaderBuilder(reader).build()
        ) {
            String[] headers;
            if (EmptyKit.isNotBlank(csvConfig.getHeader())) {
                headers = csvConfig.getHeader().split(",");
            } else {
                csvReader.skip(csvConfig.getDataStartLine() - 1);
                String[] data = csvReader.readNext();
                if (csvConfig.getIncludeHeader()) {
                    headers = data;
                } else {
                    headers = new String[data.length];
                    for (int j = 0; j < headers.length; j++) {
                        headers[j] = "column" + (j + 1);
                    }
                }
            }
            if (EmptyKit.isNotEmpty(headers)) {
                csvReader.skip(csvOffset.getDataLine() - 1 - csvReader.getSkipLines());
                String[] data;
                while (isAlive() && (data = csvReader.readNext()) != null) {
                    Map<String, Object> after = new HashMap<>();
                    putIntoMap(after, headers, data, dataTypeMap);
                    tapEvents.get().add(insertRecordEvent(after, tapTable.getId()));
                    if (tapEvents.get().size() == eventBatchSize) {
                        csvOffset.setDataLine(csvOffset.getDataLine() + eventBatchSize);
                        csvOffset.setPath(csvOffset.getPath());
                        eventsOffsetConsumer.accept(tapEvents.get(), csvOffset);
                        tapEvents.set(list());
                    }
                }
            }
        }
    }

    private void putIntoMap(Map<String, Object> after, String[] headers, String[] data, Map<String, String> dataTypeMap) {
        for (int i = 0; i < headers.length && i < data.length; i++) {
            switch (dataTypeMap.get(headers[i])) {
                case "BOOLEAN":
                    after.put(headers[i], "true".equalsIgnoreCase(data[i]));
                    break;
                case "INTEGER":
                    after.put(headers[i], Integer.parseInt(data[i]));
                    break;
                case "NUMBER":
                    after.put(headers[i], new BigDecimal(data[i]));
                    break;
                case "DATETIME":
                    after.put(headers[i], DateUtil.parse(data[i]));
                    break;
                default:
                    after.put(headers[i], data[i]);
                    break;
            }
        }
        for (int i = 0; i < headers.length - data.length; i++) {
            switch (dataTypeMap.get(headers[i + data.length])) {
                case "STRING":
                case "TEXT":
                    after.put(headers[i + data.length], "");
                    break;
                default:
                    after.put(headers[i + data.length], null);
                    break;
            }
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        CsvOffset csvOffset = (CsvOffset) offsetState;
        Map<String, Long> allLastModified = csvOffset.getAllLastModified();
        while (isAlive()) {
            int sleep = 60;
            while (isAlive() && (sleep-- > 0)) {
                TapSimplify.sleep(1000);
            }
            Map<String, Long> newLastModified = getFilteredFiles().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getLastModified()));
            AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(new ArrayList<>());
            Iterator<Map.Entry<String, Long>> iterator = newLastModified.entrySet().iterator();
            while (isAlive() && iterator.hasNext()) {
                Map.Entry<String, Long> entry = iterator.next();
                if (!allLastModified.containsKey(entry.getKey())) {
                    continue;
                }
                if (allLastModified.get(entry.getKey()) < entry.getValue()) {
                    readOneFile(csvOffset, nodeContext.getTableMap().get(tableList.get(0)), recordSize, consumer, tapEvents);
                }
            }
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Exception {
        Map<String, TapFile> fileMap = getFilteredFiles();
        CsvOffset csvOffset = new CsvOffset();
        csvOffset.setAllLastModified(fileMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getLastModified())));
        return csvOffset;
    }

}
