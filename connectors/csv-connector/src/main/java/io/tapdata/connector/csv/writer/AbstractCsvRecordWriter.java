package io.tapdata.connector.csv.writer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.tapdata.common.AbstractFileRecordWriter;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractCsvRecordWriter extends AbstractFileRecordWriter {

    public AbstractCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) throws Exception {
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

    protected synchronized CsvFileWriter getCsvFileWriterAndInit(String uniquePath) throws Exception {
        CsvFileWriter csvFileWriter;
        CsvConfig csvConfig = (CsvConfig) fileConfig;
        if (fileWriterMap.containsKey(uniquePath)) {
            csvFileWriter = (CsvFileWriter) fileWriterMap.get(uniquePath);
            if (csvFileWriter.isClosed()) {
                if (storage.supportAppendData()) {
                    csvFileWriter.init();
                } else {
                    csvFileWriter = new CsvFileWriter(storage, getNewCacheFileName(csvFileWriter.getPath()), fileConfig.getFileEncoding());
                    csvFileWriter.setRule(csvConfig.getSeparator().replaceAll("\\[", "").replaceAll("]", "").charAt(0),
                            csvConfig.getQuoteChar().charAt(0), csvConfig.getLineEnd());
                    csvFileWriter.init();
                    fileWriterMap.put(uniquePath, csvFileWriter);
                }
            }
        } else {
            csvFileWriter = new CsvFileWriter(storage, uniquePath, fileConfig.getFileEncoding());
            csvFileWriter.setRule(csvConfig.getSeparator().replaceAll("\\[", "").replaceAll("]", "").charAt(0),
                    csvConfig.getQuoteChar().charAt(0), csvConfig.getLineEnd());
            csvFileWriter.init();
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
            lastWriteMap.put(uniquePath, System.currentTimeMillis());
        }
        return csvFileWriter;
    }

    private String getNewCacheFileName(String path) {
        if (!path.contains(".tapCache")) {
            return path + ".tapCache1";
        } else {
            int index = path.lastIndexOf(".tapCache") + 9;
            return path.substring(0, index) + (Integer.parseInt(path.substring(index)) + 1);
        }
    }

    protected String[] getStringArray(Map<String, Object> data, List<String> fieldList, String op) {
        String[] res = fieldList.stream().map(v -> EmptyKit.isNull(data.get(v)) ? "" : String.valueOf(data.get(v))).toArray(String[]::new);
        res = Arrays.copyOf(res, fieldList.size() + 1);
        res[fieldList.size()] = op;
        return res;
    }

    @Override
    protected void writeCacheFile(String coreLocalFilePath, List<TapFile> cacheFilesPath) throws Exception {
        try (
                CsvFileWriter localCsvFileWriter = new CsvFileWriter(localStorage, coreLocalFilePath, fileConfig.getFileEncoding())
        ) {
            for (TapFile file : cacheFilesPath) {
                storage.readFile(file.getPath(), inputStream -> {
                    try (
                            Reader reader = new InputStreamReader(inputStream, fileConfig.getFileEncoding());
                            CSVReader csvReader = new CSVReaderBuilder(reader).build()
                    ) {
                        String[] data;
                        while ((data = csvReader.readNext()) != null) {
                            localCsvFileWriter.getCsvWriter().writeNext(data);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }
}
