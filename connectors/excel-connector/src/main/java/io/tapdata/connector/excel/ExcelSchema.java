package io.tapdata.connector.excel;

import io.tapdata.common.FileConfig;
import io.tapdata.common.FileSchema;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.util.Map;

public class ExcelSchema extends FileSchema {

    public ExcelSchema(FileConfig fileConfig, TapFileStorage storage) {
        super(fileConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {

    }

}
