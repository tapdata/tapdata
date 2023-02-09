package io.tapdata.connector.csv.writer;

import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFileStorage;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.Consumer;

public class DateCsvRecordWriter extends AbstractCsvRecordWriter {

    public DateCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) throws Exception {
        super(storage, csvConfig, tapTable, kvMap);
    }

    @Override
    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        String fileNameExpression = fileConfig.getFileNameExpression();
        String newWriteDateString = replaceDateSign(fileNameExpression);
        if (!newWriteDateString.equals(writeDateString)) {
            fileWriterMap.forEach((k, v) -> v.close());
            fileWriterMap.clear();
            kvMap.clear();
            writeDateString = newWriteDateString;
        }
        String datePath = correctPath(fileConfig.getWriteFilePath()) + newWriteDateString;
        if (!fileNameExpression.contains("${record.")) {
            writeOneFile(tapRecordEvents, writeListResultConsumer, datePath);
        } else {
            writeMultiFiles(tapRecordEvents, writeListResultConsumer, datePath);
        }
    }
}
