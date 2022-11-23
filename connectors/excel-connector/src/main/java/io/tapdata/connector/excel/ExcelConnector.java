package io.tapdata.connector.excel;

import io.tapdata.common.FileConnector;
import io.tapdata.common.FileOffset;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.ExcelUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.file.TapFile;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_excel.json")
public class ExcelConnector extends FileConnector {

    @Override
    protected void initConnection(TapConnectionContext connectorContext) throws Exception {
        fileConfig = new ExcelConfig();
        super.initConnection(connectorContext);
    }

    @Override
    protected void makeFileOffset(FileOffset fileOffset) {
        fileOffset.setSheetNum(((ExcelConfig) fileConfig).getSheetNum().stream().findFirst().orElse(1));
        fileOffset.setDataLine(fileConfig.getDataStartLine());
    }

    @Override
    protected void readOneFile(FileOffset fileOffset, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer, AtomicReference<List<TapEvent>> tapEvents) throws Exception {
        ExcelConfig excelConfig = (ExcelConfig) fileConfig;
        Object[] headers = tapTable.getNameFieldMap().values().stream().map(TapField::getName).toArray();
        try (
                Workbook wb = WorkbookFactory.create(storage.readFile(fileOffset.getPath()), excelConfig.getExcelPassword())
        ) {
            List<Integer> sheetNumbers = EmptyKit.isEmpty(excelConfig.getSheetNum()) ? ExcelUtil.getAllSheetNumber(wb.getNumberOfSheets()) : excelConfig.getSheetNum();
            List<Integer> sheets = sheetNumbers.stream().filter(n -> n >= fileOffset.getSheetNum()).collect(Collectors.toList());
            for (int i = 0; isAlive() && i < sheets.size(); i++) {
                Sheet sheet = wb.getSheetAt(sheets.get(i) - 1);
                fileOffset.setSheetNum(sheets.get(i));
                fileOffset.setDataLine(excelConfig.getDataStartLine());
                for (int j = fileOffset.getDataLine() - 1; isAlive() && j <= sheet.getLastRowNum(); j++) {
                    Row row = sheet.getRow(j);
                    Map<String, Object> after = new HashMap<>();
                    for (int k = excelConfig.getFirstColumn() - 1; k < excelConfig.getLastColumn(); k++) {
                        after.put((String) headers[k - excelConfig.getFirstColumn() + 1], ExcelUtil.getCellValue(row.getCell(k), null));
                    }
                    tapEvents.get().add(insertRecordEvent(after, tapTable.getId()));
                    if (tapEvents.get().size() == eventBatchSize) {
                        fileOffset.setDataLine(fileOffset.getDataLine() + eventBatchSize);
                        fileOffset.setPath(fileOffset.getPath());
                        eventsOffsetConsumer.accept(tapEvents.get(), fileOffset);
                        tapEvents.set(list());
                    }
                }
            }
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        initConnection(connectionContext);
        TapTable tapTable = table(fileConfig.getModelName());
        ConcurrentMap<String, TapFile> csvFileMap = getFilteredFiles();
        ExcelSchema excelSchema = new ExcelSchema((ExcelConfig) fileConfig, storage);
        Map<String, Object> sample;
        //excel has column header
        if (EmptyKit.isNotBlank(fileConfig.getHeader())) {
            sample = excelSchema.sampleFixedFileData(csvFileMap);
        } else //analyze every excel file
        {
            sample = excelSchema.sampleEveryFileData(csvFileMap);
        }
        if (EmptyKit.isEmpty(sample)) {
            throw new RuntimeException("Load schema from csv files error: no headers and contents!");
        }
        makeTapTable(tapTable, sample, fileConfig.getJustString());
        consumer.accept(Collections.singletonList(tapTable));
        storage.destroy();
    }

}
