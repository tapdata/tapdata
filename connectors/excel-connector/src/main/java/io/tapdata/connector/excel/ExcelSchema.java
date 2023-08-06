package io.tapdata.connector.excel;

import io.tapdata.common.FileSchema;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ExcelSchema extends FileSchema {

    private final static String TAG = ExcelSchema.class.getSimpleName();

    public ExcelSchema(ExcelConfig excelConfig, TapFileStorage storage) {
        super(excelConfig, storage);
    }

    @Override
    protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception {
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        storage.readFile(tapFile.getPath(), is -> {
            try (
                    Workbook wb = WorkbookFactory.create(is, excelConfig.getExcelPassword())
            ) {
                FormulaEvaluator formulaEvaluator = wb.getCreationHelper().createFormulaEvaluator();
                List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
                for (Integer num : sheetNumbers) {
                    Sheet sheet = wb.getSheetAt(num - 1);
                    List<CellRangeAddress> mergedList = sheet.getMergedRegions();
                    Map<CellRangeAddress, Cell> mergedDataMap = ExcelUtil.getMergedDataMap(sheet);
                    if (excelConfig.getHeaderLine() > 0) {
                        Row headerRow = sheet.getRow(excelConfig.getHeaderLine() - 1);
                        if (EmptyKit.isNull(headerRow)) {
                            continue;
                        }
                        Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                        for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                            Object headCell = ExcelUtil.getMergedCellValue(mergedList, mergedDataMap, headerRow.getCell(i), formulaEvaluator);
                            putValidIntoMap(sampleResult, (EmptyKit.isNull(headCell) || EmptyKit.isBlank(String.valueOf(headCell))) ? "column" + (i - excelConfig.getFirstColumn() + 2) : String.valueOf(headCell),
                                    ExcelUtil.getMergedCellValue(mergedList, mergedDataMap, dataRow.getCell(i), formulaEvaluator));
                        }
                    } else {
                        Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                        if (EmptyKit.isNull(dataRow)) {
                            continue;
                        }
                        for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                            putValidIntoMap(sampleResult, "column" + (i - excelConfig.getFirstColumn() + 2),
                                    ExcelUtil.getMergedCellValue(mergedList, mergedDataMap, dataRow.getCell(i), formulaEvaluator));
                        }
                    }
                }
            } catch (Exception e) {
                TapLogger.error(TAG, "read excel file error!", e);
            }
        });
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> excelFileMap) throws Exception {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        String[] headers = excelConfig.getHeader().split(",");
        if (EmptyKit.isEmpty(excelFileMap)) {
            putIntoMap(headers, null, sampleResult);
        } else {
            for (String path : excelFileMap.keySet().stream().sorted().collect(Collectors.toList())) {
                AtomicBoolean needStop = new AtomicBoolean(false);
                storage.readFile(path, is -> {
                    try (
                            Workbook wb = WorkbookFactory.create(is, excelConfig.getExcelPassword())
                    ) {
                        FormulaEvaluator formulaEvaluator = wb.getCreationHelper().createFormulaEvaluator();
                        List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
                        for (Integer num : sheetNumbers) {
                            Sheet sheet = wb.getSheetAt(num - 1);
                            List<CellRangeAddress> mergedList = sheet.getMergedRegions();
                            Map<CellRangeAddress, Cell> mergedDataMap = ExcelUtil.getMergedDataMap(sheet);
                            Row dataRow = sheet.getRow(excelConfig.getDataStartLine() - 1);
                            if (EmptyKit.isNull(dataRow)) {
                                continue;
                            }
                            for (int i = excelConfig.getFirstColumn() - 1; i < excelConfig.getLastColumn(); i++) {
                                sampleResult.put(headers[i - excelConfig.getFirstColumn() + 1], ExcelUtil.getMergedCellValue(mergedList, mergedDataMap, dataRow.getCell(i), formulaEvaluator));
                            }
                            break;
                        }
                        if (EmptyKit.isNotEmpty(sampleResult)) {
                            needStop.set(true);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                if (needStop.get()) {
                    break;
                }
            }
        }
        return sampleResult;
    }

    @Override
    protected synchronized void putValidIntoMap(Map<String, Object> map, String key, Object value) {
        if (!map.containsKey(key) || EmptyKit.isNull(map.get(key))) {
            map.put(key, value);
        }
    }

}
