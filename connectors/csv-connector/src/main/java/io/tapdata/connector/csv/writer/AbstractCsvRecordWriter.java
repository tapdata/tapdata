package io.tapdata.connector.csv.writer;

import io.tapdata.common.AbstractFileRecordWriter;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractCsvRecordWriter extends AbstractFileRecordWriter {

    public AbstractCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) {
        super(storage, csvConfig, tapTable, kvMap);
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
        kvMap.put("tapdata_file_last_write", lastWriteMap);
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
        fileWriterMap.forEach((k, v) -> {
            v.flush();
            if (System.currentTimeMillis() - lastWriteMap.get(k) > 3 * 60 * 1000) {
                v.close();
            }
        });
        kvMap.put("tapdata_file_last_write", lastWriteMap);
        writeListResultConsumer.accept(listResult
                .insertedCount(insert)
                .modifiedCount(update)
                .removedCount(delete));
    }

    protected CsvFileWriter getCsvFileWriterAndInit(String uniquePath) throws Exception {
        CsvFileWriter csvFileWriter;
        if (fileWriterMap.containsKey(uniquePath)) {
            csvFileWriter = (CsvFileWriter) fileWriterMap.get(uniquePath);
            if (csvFileWriter.isClosed()) {
                csvFileWriter.init();
            }
        } else {
            csvFileWriter = new CsvFileWriter(storage, uniquePath, fileConfig.getFileEncoding());
            fileWriterMap.put(uniquePath, csvFileWriter);
        }
        if (!lastWriteMap.containsKey(uniquePath) || EmptyKit.isNull(lastWriteMap.get(uniquePath))) {
            for (int i = 0; i < fileConfig.getHeaderLine() - 1; i++) {
                csvFileWriter.getCsvWriter().writeNext(null);
            }
            if (EmptyKit.isBlank(fileConfig.getHeader())) {
                csvFileWriter.getCsvWriter().writeNext(fieldList.toArray(new String[0]));
            } else {
                csvFileWriter.getCsvWriter().writeNext(fileConfig.getHeader().split(","));
            }
        }
        return csvFileWriter;
    }

    protected String[] getStringArray(Map<String, Object> data, List<String> fieldList, String op) {
        String[] res = fieldList.stream().map(v -> EmptyKit.isNull(data.get(v)) ? "" : String.valueOf(data.get(v))).toArray(String[]::new);
        res = Arrays.copyOf(res, fieldList.size() + 1);
        res[fieldList.size()] = op;
        return res;
    }
}
