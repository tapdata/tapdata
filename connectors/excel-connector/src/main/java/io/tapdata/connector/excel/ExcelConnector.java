package io.tapdata.connector.excel;

import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressBase;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_excel.json")
public class ExcelConnector extends FileConnector {

    private static final String TAG = ExcelConnector.class.getSimpleName();

    static {
        try {
            XSSFWorkbookFactory factory = new XSSFWorkbookFactory();
            WorkbookFactory.addProvider(factory);
        } catch (Throwable throwable) {
            TapLogger.error(TAG, "Add provider XSSFWorkbookFactory failed, {}", InstanceFactory.instance(TapUtils.class).getStackTrace(throwable));
        }
    }

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new ExcelConfig();
        super.initConnection(connectorContext);
    }

    @Override
    protected void makeFileOffset(FileOffset fileOffset) {
        List<Integer> sheetNumber = ((ExcelConfig) fileConfig).getSheetNum();
        fileOffset.setSheetNum(EmptyKit.isEmpty(sheetNumber) ? 1 : sheetNumber.stream().findFirst().orElse(1));
        fileOffset.setDataLine(fileConfig.getDataStartLine());
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        Object[] headers = tapTable.getNameFieldMap().values().stream().map(TapField::getName).toArray();
        long lastModified = storage.getFile(fileOffset.getPath()).getLastModified();
        storage.readFile(fileOffset.getPath(), is -> {
            try (
                    Workbook wb = WorkbookFactory.create(is, excelConfig.getExcelPassword())
            ) {
                FormulaEvaluator formulaEvaluator = wb.getCreationHelper().createFormulaEvaluator();
                List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
                List<Integer> sheets = sheetNumbers.stream().filter(n -> n >= fileOffset.getSheetNum()).collect(Collectors.toList());
                for (int i = 0; isAlive() && i < sheets.size(); i++) {
                    Sheet sheet = wb.getSheetAt(sheets.get(i) - 1);
                    List<CellRangeAddress> mergedList = sheet.getMergedRegions();
                    int lastMergedRow = mergedList.stream().map(CellRangeAddressBase::getLastRow).max(Comparator.naturalOrder()).orElse(-1);
                    Map<CellRangeAddress, Cell> mergedDataMap = ExcelUtil.getMergedDataMap(sheet);
                    fileOffset.setSheetNum(sheets.get(i));
                    fileOffset.setDataLine(excelConfig.getDataStartLine());
                    for (int j = fileOffset.getDataLine() - 1; isAlive() && j <= sheet.getLastRowNum(); j++) {
                        Row row = sheet.getRow(j);
                        if (EmptyKit.isNull(row)) {
                            break;
                        }
                        Map<String, Object> after = new HashMap<>();
                        if (j > lastMergedRow) {
                            for (int k = excelConfig.getFirstColumn() - 1; k < excelConfig.getLastColumn(); k++) {
                                Object val = ExcelUtil.getCellValue(row.getCell(k), formulaEvaluator);
                                after.put((String) headers[k - excelConfig.getFirstColumn() + 1], excelConfig.getJustString() ? (EmptyKit.isNull(val) ? "null" : String.valueOf(val)) : val);
                            }
                        } else {
                            for (int k = excelConfig.getFirstColumn() - 1; k < excelConfig.getLastColumn(); k++) {
                                Object val = ExcelUtil.getMergedCellValue(mergedList, mergedDataMap, row.getCell(k), formulaEvaluator);
                                after.put((String) headers[k - excelConfig.getFirstColumn() + 1], excelConfig.getJustString() ? (EmptyKit.isNull(val) ? "null" : String.valueOf(val)) : val);
                            }
                        }
                        tapEvents.get().add(insertRecordEvent(after, tapTable.getId()).referenceTime(lastModified));
                        if (tapEvents.get().size() == eventBatchSize) {
                            fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize);
                            fileOffset.setPath(fileOffset.getPath());
                            eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                            tapEvents.set(list());
                        }
                    }
                }
            } catch (IOException e) {
                TapLogger.warn(TAG, String.format("Reading file %s occurs error, skip it", fileOffset.getPath()), e);
            }
        });
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "STRING", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "STRING", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext);
        if (EmptyKit.isBlank(fileConfig.getModelName())) {
            return;
        }
        TapTable tapTable = table(fileConfig.getModelName());
        ConcurrentMap<String, TapFile> excelFileMap = getFilteredFiles();
        ExcelSchema excelSchema = new ExcelSchema((ExcelConfig) fileConfig, storage);
        Map<String, Object> sample;
        //excel has column header
        if (EmptyKit.isNotBlank(fileConfig.getHeader())) {
            sample = excelSchema.sampleFixedFileData(excelFileMap);
        } else //analyze every excel file
        {
            sample = excelSchema.sampleEveryFileData(excelFileMap);
        }
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from excel files error: no headers and contents!");
        }
        makeTapTable(tapTable, sample, fileConfig.getJustString());
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }

    @Override
    protected void makeTapTable(TapTable tapTable, Map<String, Object> sample, boolean isJustString) {
        for (Map.Entry<String, Object> objectEntry : sample.entrySet()) {
            TapField field = new TapField();
            field.name(objectEntry.getKey().replaceAll("\n", ""));
            Object val = objectEntry.getValue();
            if (isJustString) {
                val = EmptyKit.isNull(val) ? "null" : String.valueOf(val);
            }
            if (EmptyKit.isNull(val) || val instanceof String) {
                if (EmptyKit.isNotEmpty((String) val) && ((String) val).length() > 200) {
                    field.dataType("TEXT");
                } else {
                    field.dataType("STRING");
                }
            } else {
                field.dataType(val.getClass().getSimpleName().toUpperCase());
            }
            tapTable.add(field);
        }
    }

}
