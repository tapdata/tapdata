package io.tapdata.connector.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.common.FileSchema;
import io.tapdata.common.util.MatchUtil;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.connector.csv.writer.DateCsvRecordWriter;
import io.tapdata.connector.csv.writer.RecordCsvRecordWriter;
import io.tapdata.connector.csv.writer.UniqueCsvRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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

    private OffStandardFilter offStandardFilter;

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new CsvConfig();
        super.initConnection(connectorContext);
        offStandardFilter = new OffStandardFilter(((CsvConfig) fileConfig).getLineExpression());
    }

    @Override
    protected void makeFileOffset(FileOffset fileOffset) {
        fileOffset.setDataLine(fileConfig.getDataStartLine());
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

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportWriteRecord(this::writeRecord);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        //as file-connector: nodeConfig is supported, so initConnection can be used
        initConnection(connectionContext);
        if (EmptyKit.isBlank(fileConfig.getModelName())) {
            return;
        }
        TapTable tapTable = table(fileConfig.getModelName());
        ConcurrentMap<String, TapFile> csvFileMap = getFilteredFiles();
        FileSchema csvSchema;
        if (((CsvConfig) fileConfig).getOffStandard()) {
            csvSchema = new OffStandardCsvSchema((CsvConfig) fileConfig, storage);
        } else {
            csvSchema = new CsvSchema((CsvConfig) fileConfig, storage);
        }
        Map<String, Object> sample;
        //csv has column header
        if (EmptyKit.isNotBlank(fileConfig.getHeader())) {
            sample = csvSchema.sampleFixedFileData(csvFileMap);
        } else //analyze every csv file
        {
            sample = csvSchema.sampleEveryFileData(csvFileMap);
        }
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from csv files error: no headers and contents!");
        }
        makeTapTable(tapTable, sample, fileConfig.getJustString());
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }

    protected void readOneFile(FileOffset fileOffset,
                               TapTable tapTable,
                               int eventBatchSize,
                               BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                               AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        if (((CsvConfig) fileConfig).getOffStandard()) {
            readOffStandardCsv(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        } else {
            readStandardCsv(fileOffset, tapTable, eventBatchSize, eventsOffsetConsumer, tapEvents);
        }
    }

    private void readStandardCsv(FileOffset fileOffset,
                                 TapTable tapTable,
                                 int eventBatchSize,
                                 BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                                 AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        Map<String, String> dataTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    CSVReader csvReader = new CSVReaderBuilder(reader).build()
            ) {
                String[] headers;
                if (EmptyKit.isNotBlank(fileConfig.getHeader())) {
                    headers = fileConfig.getHeader().split(",");
                } else {
                    if (fileConfig.getHeaderLine() > 0) {
                        csvReader.skip(fileConfig.getHeaderLine() - 1);
                        headers = csvReader.readNext();
                    } else {
                        csvReader.skip(fileConfig.getDataStartLine() - 1);
                        String[] data = csvReader.readNext();
                        headers = new String[data.length];
                        for (int j = 0; j < headers.length; j++) {
                            headers[j] = "column" + (j + 1);
                        }
                    }
                }
                if (EmptyKit.isNotEmpty(headers)) {
                    csvReader.skip(fileOffset.getDataLine() - 2 - csvReader.getSkipLines());
                    String[] data;
                    int blankSkip = 0;
                    while (isAlive() && (data = csvReader.readNext()) != null) {
                        Map<String, Object> after = new HashMap<>();
                        putIntoMap(after, headers, data, dataTypeMap);
                        if (after.entrySet().stream().allMatch(v -> EmptyKit.isNull(v.getValue()))) {
                            blankSkip++;
                            continue;
                        }
                        tapEvents.get().add(insertRecordEvent(after, tapTable.getId()).referenceTime(lastModified));
                        if (tapEvents.get().size() == eventBatchSize) {
                            fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize + blankSkip);
                            blankSkip = 0;
                            fileOffset.setPath(fileOffset.getPath());
                            eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                            tapEvents.set(list());
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void readOffStandardCsv(FileOffset fileOffset,
                                    TapTable tapTable,
                                    int eventBatchSize,
                                    BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                                    AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        Map<String, String> dataTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Reader reader = new InputStreamReader(is, fileConfig.getFileEncoding());
                    BufferedReader bufferedReader = new BufferedReader(reader)
            ) {
                String[] headers;
                int skip = 0;
                if (EmptyKit.isNotBlank(fileConfig.getHeader())) {
                    headers = fileConfig.getHeader().split(",");
                } else {
                    if (fileConfig.getHeaderLine() > 0) {
                        skipRead(bufferedReader, fileConfig.getHeaderLine() - 1);
                        skip += fileConfig.getHeaderLine() - 1;
                        headers = offStandardFilter.filter(bufferedReader.readLine());
                    } else {
                        skipRead(bufferedReader, fileConfig.getDataStartLine() - 1);
                        skip += fileConfig.getDataStartLine() - 1;
                        String[] data = offStandardFilter.filter(bufferedReader.readLine());
                        headers = new String[data.length];
                        for (int j = 0; j < headers.length; j++) {
                            headers[j] = "column" + (j + 1);
                        }
                    }
                }
                if (EmptyKit.isNotEmpty(headers)) {
                    skipRead(bufferedReader, fileOffset.getDataLine() - 2 - skip);
                    String[] data;
                    int blankSkip = 0;
                    while (isAlive() && (data = offStandardFilter.filter(bufferedReader.readLine())) != null) {
                        Map<String, Object> after = new HashMap<>();
                        putIntoMap(after, headers, data, dataTypeMap);
                        if (after.entrySet().stream().allMatch(v -> EmptyKit.isNull(v.getValue()))) {
                            blankSkip++;
                            continue;
                        }
                        tapEvents.get().add(insertRecordEvent(after, tapTable.getId()).referenceTime(lastModified));
                        if (tapEvents.get().size() == eventBatchSize) {
                            fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize + blankSkip);
                            blankSkip = 0;
                            fileOffset.setPath(fileOffset.getPath());
                            eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                            tapEvents.set(list());
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void skipRead(BufferedReader bufferedReader, int skip) throws IOException {
        while (skip > 0) {
            bufferedReader.readLine();
            skip--;
        }
    }

    private void putIntoMap(Map<String, Object> after, String[] headers, String[] data, Map<String, String> dataTypeMap) {
        for (int i = 0; i < headers.length && i < data.length; i++) {
            try {
                after.put(headers[i], MatchUtil.parse(data[i], dataTypeMap.get(headers[i])));
            } catch (Exception e) {
                throw new RuntimeException(String.format("%s field has invalid value", headers[i]), e);
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

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        synchronized (this) {
            if (EmptyKit.isNull(fileRecordWriter)) {
                String fileNameExpression = fileConfig.getFileNameExpression();
                if (EmptyKit.isBlank(fileNameExpression) || (!fileNameExpression.contains("${date:") && !fileNameExpression.contains("${record."))) {
                    fileRecordWriter = new UniqueCsvRecordWriter(storage, (CsvConfig) fileConfig, tapTable, connectorContext.getStateMap());
                } else if (fileNameExpression.contains("${date:")) {
                    fileRecordWriter = new DateCsvRecordWriter(storage, (CsvConfig) fileConfig, tapTable, connectorContext.getStateMap());
                } else {
                    fileRecordWriter = new RecordCsvRecordWriter(storage, (CsvConfig) fileConfig, tapTable, connectorContext.getStateMap());
                }
                fileRecordWriter.setConnectorId(firstConnectorId);
            }
        }
        fileRecordWriter.write(tapRecordEvents, writeListResultConsumer);
    }
}
