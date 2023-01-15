package io.tapdata.connector.csv.writer;

import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFileStorage;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.List;
import java.util.function.Consumer;

public class RecordCsvRecordWriter extends AbstractCsvRecordWriter {

    public RecordCsvRecordWriter(TapFileStorage storage, CsvConfig csvConfig, TapTable tapTable, KVMap<Object> kvMap) throws Exception {
        super(storage, csvConfig, tapTable, kvMap);
    }

    @Override
    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Exception {
        writeMultiFiles(tapRecordEvents, writeListResultConsumer, correctPath(fileConfig.getWriteFilePath()) + fileConfig.getFileNameExpression());
    }
}
