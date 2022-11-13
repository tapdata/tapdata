package io.tapdata.connector.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
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
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.util.DateUtil;

import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_csv.json")
public class CsvConnector extends FileConnector {

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new CsvConfig();
        super.initConnection(connectorContext);
    }

    @Override
    protected void makeFileOffset(FileOffset fileOffset) {
        fileOffset.setDataLine(((CsvConfig) fileConfig).getDataStartLine() + (((CsvConfig) fileConfig).getIncludeHeader() ? 1 : 0));
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
        TapTable tapTable = table(fileConfig.getModelName());
        ConcurrentMap<String, TapFile> csvFileMap = getFilteredFiles();
        CsvSchema csvSchema = new CsvSchema((CsvConfig) fileConfig, storage);
        Map<String, Object> sample;
        //csv has column header
        if (EmptyKit.isNotBlank(((CsvConfig) fileConfig).getHeader())) {
            sample = csvSchema.sampleFixedFileData(csvFileMap);
        } else //analyze every csv file
        {
            sample = csvSchema.sampleEveryFileData(csvFileMap);
        }
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from csv files error: no headers and contents!");
        }
        if (((CsvConfig) fileConfig).getJustString()) {
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

    protected void readOneFile(FileOffset fileOffset,
                               TapTable tapTable,
                               int eventBatchSize,
                               BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                               AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        Map<String, String> dataTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
        try (
                Reader reader = new InputStreamReader(storage.readFile(fileOffset.getPath()));
                CSVReader csvReader = new CSVReaderBuilder(reader).build()
        ) {
            String[] headers;
            if (EmptyKit.isNotBlank(((CsvConfig) fileConfig).getHeader())) {
                headers = ((CsvConfig) fileConfig).getHeader().split(",");
            } else {
                csvReader.skip(((CsvConfig) fileConfig).getDataStartLine() - 1);
                String[] data = csvReader.readNext();
                if (((CsvConfig) fileConfig).getIncludeHeader()) {
                    headers = data;
                } else {
                    headers = new String[data.length];
                    for (int j = 0; j < headers.length; j++) {
                        headers[j] = "column" + (j + 1);
                    }
                }
            }
            if (EmptyKit.isNotEmpty(headers)) {
                csvReader.skip(fileOffset.getDataLine() - 1 - csvReader.getSkipLines());
                String[] data;
                while (isAlive() && (data = csvReader.readNext()) != null) {
                    Map<String, Object> after = new HashMap<>();
                    putIntoMap(after, headers, data, dataTypeMap);
                    tapEvents.get().add(insertRecordEvent(after, tapTable.getId()));
                    if (tapEvents.get().size() == eventBatchSize) {
                        fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize);
                        fileOffset.setPath(fileOffset.getPath());
                        eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
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

}
