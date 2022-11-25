package io.tapdata.connector.excel;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExcelSchema extends FileSchema {

    private final static String TAG = ExcelSchema.class.getSimpleName();

    public ExcelSchema(ExcelConfig excelConfig, TapFileStorage storage) {
        super(excelConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        try (
                Workbook wb = WorkbookFactory.create(storage.readFile(tapFile.getPath()), excelConfig.getExcelPassword())
        ) {
            List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
            for (Integer num : sheetNumbers) {
                Sheet sheet = wb.getSheetAt(num - 1);
                if (excelConfig.getHeaderLine() > 0) {
                    Row headerRow = sheet.getRow(excelConfig.getHeaderLine() - 1);
                    if (EmptyKit.isNull(headerRow)) {
                        continue;
                    }
                    Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                    for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                        putValidIntoMap(sampleResult, String.valueOf(ExcelUtil.getCellValue(headerRow.getCell(i), null)), ExcelUtil.getCellValue(dataRow.getCell(i), null));
                    }
                } else {
                    Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                    if (EmptyKit.isNull(dataRow)) {
                        continue;
                    }
                    for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                        putValidIntoMap(sampleResult, "column" + (i - excelConfig.getFirstColumn() + 2), ExcelUtil.getCellValue(dataRow.getCell(i), null));
                    }
                }
            }
        } catch (Exception e) {
            TapLogger.error(TAG, "read excel file error!", e);
        }
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> excelFileMap) throws Exception {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        String[] headers = excelConfig.getHeader().split(",");
        if (EmptyKit.isEmpty(excelFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : excelFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                try (
                        Workbook wb = WorkbookFactory.create(storage.readFile(path), excelConfig.getExcelPassword())
                ) {
                    List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
                    for (Integer num : sheetNumbers) {
                        Sheet sheet = wb.getSheetAt(num - 1);
                        Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                        if (EmptyKit.isNull(dataRow)) {
                            continue;
                        }
                        for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                            sampleResult.put(headers[i - excelConfig.getFirstColumn() + 1], ExcelUtil.getCellValue(dataRow.getCell(i), null));
                        }
                        break;
                    }
                    if (EmptyKit.isNotEmpty(sampleResult)) {
                        break;
                    }
                }
            }
        }
        return sampleResult;
    }

}
