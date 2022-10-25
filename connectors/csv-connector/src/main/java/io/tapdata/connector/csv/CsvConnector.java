package io.tapdata.connector.csv;

import io.tapdata.base.ConnectorBase;
import io.tapdata.common.FileProtocolEnum;
import io.tapdata.common.FileTest;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
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
        Map<String, String> sample;
        //csv has column header
        if (EmptyKit.isNotBlank(csvConfig.getHeader())) {
            sample = csvSchema.sampleFixedFileData(csvFileMap);
        } else //analyze every csv file
        {
            sample = csvSchema.sampleEveryFileData(csvFileMap);
        }
        for (String col : csvConfig.getHeader().split(",")) {
            TapField field = new TapField();
            field.name(col);

        }

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

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        return 0;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {

    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {

    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return null;
    }

}
