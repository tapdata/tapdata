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

    public DateCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) {
        super(storage, csvConfig, tapTable, kvMap);
    }

    @Override
    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        String fileNameExpression = csvConfig.getFileNameExpression();
        String newWriteDateString = replaceDateSign(fileNameExpression);
        if (!newWriteDateString.equals(writeDateString)) {
            csvFileWriterMap.forEach((k, v) -> v.close());
            csvFileWriterMap.clear();
            kvMap.clear();
            writeDateString = newWriteDateString;
        }
        String datePath = correctPath(csvConfig.getWriteFilePath()) + newWriteDateString;
        if (!fileNameExpression.contains("${record.")) {
            writeOneFile(tapRecordEvents, writeListResultConsumer, datePath);
        } else {
            writeMultiFiles(tapRecordEvents, writeListResultConsumer, datePath);
        }
    }
}
