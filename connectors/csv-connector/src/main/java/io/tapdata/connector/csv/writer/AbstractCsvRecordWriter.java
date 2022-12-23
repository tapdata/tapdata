package io.tapdata.connector.csv.writer;

import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class AbstractCsvRecordWriter {

    protected TapFileStorage storage;
    protected CsvConfig csvConfig;
    protected TapTable tapTable;
    protected KVMap<Object> kvMap;
    protected List<String> fieldList;
    protected Map<String, CsvFileWriter> csvFileWriterMap;
    protected String writeDateString;
    protected Map<String, Long> lastWriteMap;

    public AbstractCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) {
        csvFileWriterMap = new ConcurrentHashMap<>();
        this.storage = storage;
        this.csvConfig = csvConfig;
        this.tapTable = tapTable;
        this.kvMap = kvMap;
        this.fieldList = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        lastWriteMap = (Map<String, Long>) kvMap.get("tapdata_csv_last_write");
        if (EmptyKit.isNull(lastWriteMap)) {
            lastWriteMap = new HashMap<>();
        }
    }

    protected void writeOneFile(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, String uniquePath) throws Exception {
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        CsvFileWriter csvFileWriter = getCsvFileWriterAndInit(uniquePath);
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapInsertRecordEvent.getAfter(), fieldList, "i"));
                insert++;
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapUpdateRecordEvent.getAfter(), fieldList, "u"));
                update++;
            } else {
                TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapDeleteRecordEvent.getBefore(), fieldList, "d"));
                delete++;
            }
        }
        lastWriteMap.put(uniquePath, System.currentTimeMillis());
        csvFileWriter.flush();
        kvMap.put("tapdata_csv_last_write", lastWriteMap);
        writeListResultConsumer.accept(listResult
                .insertedCount(insert)
                .modifiedCount(update)
                .removedCount(delete));
    }

    protected void writeMultiFiles(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, String fileNameExpression) throws Exception {
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insert = 0L;
        long update = 0L;
        long delete = 0L;
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                TapInsertRecordEvent tapInsertRecordEvent = (TapInsertRecordEvent) recordEvent;
                String filePath = getFileNameFromValue(fileNameExpression, tapInsertRecordEvent.getAfter());
                CsvFileWriter csvFileWriter = getCsvFileWriterAndInit(filePath);
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapInsertRecordEvent.getAfter(), fieldList, "i"));
                lastWriteMap.put(filePath, System.currentTimeMillis());
                insert++;
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent tapUpdateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                String filePath = getFileNameFromValue(fileNameExpression, tapUpdateRecordEvent.getAfter());
                CsvFileWriter csvFileWriter = getCsvFileWriterAndInit(filePath);
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapUpdateRecordEvent.getAfter(), fieldList, "u"));
                lastWriteMap.put(filePath, System.currentTimeMillis());
                update++;
            } else {
                TapDeleteRecordEvent tapDeleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                String filePath = getFileNameFromValue(fileNameExpression, tapDeleteRecordEvent.getBefore());
                CsvFileWriter csvFileWriter = getCsvFileWriterAndInit(filePath);
                csvFileWriter.getCsvWriter().writeNext(getStringArray(tapDeleteRecordEvent.getBefore(), fieldList, "d"));
                lastWriteMap.put(filePath, System.currentTimeMillis());
                delete++;
            }
        }
        csvFileWriterMap.forEach((k, v) -> {
            v.flush();
            if (System.currentTimeMillis() - lastWriteMap.get(k) > 3 * 60 * 1000) {
                v.close();
            }
        });
        kvMap.put("tapdata_csv_last_write", lastWriteMap);
        writeListResultConsumer.accept(listResult
                .insertedCount(insert)
                .modifiedCount(update)
                .removedCount(delete));
    }

    protected CsvFileWriter getCsvFileWriterAndInit(String uniquePath) throws Exception {
        CsvFileWriter csvFileWriter;
        if (csvFileWriterMap.containsKey(uniquePath)) {
            csvFileWriter = csvFileWriterMap.get(uniquePath);
            if (csvFileWriter.isClosed()) {
                csvFileWriter.init();
            }
        } else {
            csvFileWriter = new CsvFileWriter(storage, uniquePath, csvConfig.getFileEncoding());
            csvFileWriterMap.put(uniquePath, csvFileWriter);
        }
        if (!lastWriteMap.containsKey(uniquePath) || EmptyKit.isNull(lastWriteMap.get(uniquePath))) {
            for (int i = 0; i < csvConfig.getHeaderLine() - 1; i++) {
                csvFileWriter.getCsvWriter().writeNext(null);
            }
            if (EmptyKit.isBlank(csvConfig.getHeader())) {
                csvFileWriter.getCsvWriter().writeNext(fieldList.toArray(new String[0]));
            } else {
                csvFileWriter.getCsvWriter().writeNext(csvConfig.getHeader().split(","));
            }
        }
        return csvFileWriter;
    }

    public abstract void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception;

    protected String correctPath(String path) {
        return path.endsWith("/") ? path : (path + "/");
    }

    protected String[] getStringArray(Map<String, Object> data, List<String> fieldList, String op) {
        String[] res = fieldList.stream().map(v -> EmptyKit.isNull(data.get(v)) ? "" : String.valueOf(data.get(v))).toArray(String[]::new);
        res = Arrays.copyOf(res, fieldList.size() + 1);
        res[fieldList.size()] = op;
        return res;
    }

    protected String replaceDateSign(String fileNameExpression) {
        StringBuilder res = new StringBuilder();
        Date date = new Date();
        String subStr = fileNameExpression;
        while (subStr.contains("${date:")) {
            res.append(subStr, 0, subStr.indexOf("${date:"));
            subStr = subStr.substring(subStr.indexOf("${date:") + 7);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(subStr.substring(0, subStr.indexOf("}")));
            res.append(simpleDateFormat.format(date));
            subStr = subStr.substring(subStr.indexOf("}") + 1);
        }
        res.append(subStr);
        return res.toString();
    }

    protected List<String> getKeyFieldList(List<String> fieldList) {
        List<String> keyFieldList = new ArrayList<>();
        String expression = csvConfig.getFileNameExpression();
        if (EmptyKit.isBlank(expression)) {
            return keyFieldList;
        }
        for (String field : fieldList) {
            if (expression.contains("${record." + field + "}")) {
                keyFieldList.add(field);
            }
        }
        return keyFieldList;
    }

    protected String getFileNameFromValue(String expression, Map<String, Object> value) {
        String key = expression;
        for (String field : fieldList) {
            key = key.replaceAll("\\$\\{record." + field + "}", String.valueOf(value.get(field)));
        }
        return key;
    }

    public void releaseResource() {
        csvFileWriterMap.forEach((k, v) -> v.close());
    }
}
