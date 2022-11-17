package io.tapdata.connector.excel;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ExcelSchema extends FileSchema {

    public ExcelSchema(ExcelConfig excelConfig, TapFileStorage storage) {
        super(excelConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {

    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> excelFileMap) throws Exception {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        String[] headers = fileConfig.getHeader().split(",");
        if (EmptyKit.isEmpty(excelFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : excelFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                try (
                        Workbook wb = WorkbookFactory.create(storage.readFile(path), ((ExcelConfig) fileConfig).getExcelPassword())
                ) {
                    ExcelUtil.getSheetNumber(((ExcelConfig)fileConfig).getSheetLocation()).forEach(num -> {
                        Sheet sheet = wb.getSheetAt(num);
//                        sheet.
                    });
//                    if (fileConfig.getIncludeHeader()) {
//                        csvReader.skip(fileConfig.getDataStartLine());
//                    } else {
//                        csvReader.skip(fileConfig.getDataStartLine() - 1);
//                    }
//                    String[] data = csvReader.readNext();
//                    if (EmptyKit.isNotNull(data)) {
//                        putIntoMap(headers, data, sampleResult);
//                        break;
//                    }
                }
            }
        }
        return sampleResult;
    }

}
