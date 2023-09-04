package io.tapdata.sybase.cdc.dto.analyse;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.sybase.cdc.dto.read.TableTypeEntity;
import io.tapdata.sybase.extend.ConnectionConfig;
import io.tapdata.sybase.extend.NodeConfig;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.base.ConnectorBase.fromJson;
import static io.tapdata.sybase.cdc.dto.analyse.SybaseDataTypeConvert.DELETE;
import static io.tapdata.sybase.cdc.dto.analyse.SybaseDataTypeConvert.INSERT;

/**
 * @author GavinXiao
 * @description AnalyseTapEventFromCsvString create by Gavin
 * @create 2023/7/14 10:00
 **/
public class AnalyseTapEventFromCsvString implements AnalyseRecord<String[], TapRecordEvent> {
    public static final DefaultConvert DEFAULT_CONVERT = new DefaultConvert();

    /**
     * @param tapTable {
     *                 "columnName": "sybaseType"
     *                 ....
     *                 }
     */
    @Override
    public TapRecordEvent analyse(String[] record, LinkedHashMap<String, TableTypeEntity> tapTable, String tableId, ConnectionConfig config, NodeConfig nodeConfig, CsvAnalyseFilter filter) {
        // 6,NULL,1,
        // 2023-07-13 20:43:05.0,NULL,1,
        // "sfas"",""dsafas",NULL,1,
        // 8.9,NULL,1,
        // 2023-07-13 20:43:23.0,NULL,1
        // ,4,NULL,1,
        //NULL,F,2,NULL,2023-07-25 00:00:00.0,2,NULL,2.3600000000,2,NULL,2.36,2,NULL,8,2,NULL,3.3300,2,NULL,4.3300000000,2,NULL,B│o¡Ë¼Oñ@¼qÑ┐┼ÚªrñÕªríAº┌¡nºÔÑª▒qcp850┬Óª¿utf-8,2,NULL,2023-07-25 00:00:00.0,2,NULL,3,2,NULL,BFdsd,2,NULL,:
        //                                                                                                              ED,2,NULL,3,2,NULL,V│o¡Ë¼Oñ@¼qÑ┐┼ÚªrñÕªríAº┌¡nºÔÑª▒qcp850┬Óª¿utf-8,2,NULL,215972,2,D,"{""extractorId"":0,""transactionLogPageNumber"":256566,""transactionLogRowNumber"":52,""operationLogPageNumber"":256589,""operationLogRowNumber"":0,""catalogName"":""testdb"",""timestamp"":1690280292259,""extractionTimestamp"":1690280293749,""v"":0}","{""insertCount"":7,""updateCount"":0,""deleteCount"":922,""replaceCount"":0}"
        //NULL,add one1,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,2,NULL,NULL,
        // 2,NULL,NULL,2,D,"{""extractorId"":0,""transactionLogPageNumber"":215508,""transactionLogRowNumber"":79,""operationLogPageNumber"":215508,
        // ""operationLogRowNumber"":80,""catalogName"":""testdb"",""timestamp"":1689929643121,""extractionTimestamp"":1689929643122,""v"":0}","{""insertCount"":3,
        // ""updateCount"":20000,""deleteCount"":2,""replaceCount"":0}"


        // NULL,B,2,NULL,2023-07-24 00:00:00.0,2,NULL,2.3600000000,2,NULL,2.36,2,NULL,5,2,NULL,3.3300,2,NULL,4.3300000000,2,NULL,B│o¡Ë¼Oñ@¼qÑ┐┼ÚªrñÕªríAº┌¡nºÔÑª▒qcp850┬Óª¿utf-8,2,NULL,2023-07-24 00:00:00.0,2,NULL,3,2,NULL,BFdsd,2,NULL,"cZ{""",2,NULL,3,2,NULL,V│o¡Ë¼Oñ@¼qÑ┐┼ÚªrñÕªríAº┌¡nºÔÑª▒qcp850┬Óª¿utf-8,2,
        // D,"{""extractorId"":0,""transactionLogPageNumber"":221605,""transactionLogRowNumber"":68,""operationLogPageNumber"":221605,""operationLogRowNumber"":70,""catalogName"":""testdb"",""timestamp"":1690164722442,""extractionTimestamp"":1690164722444,""v"":0}","{""insertCount"":1,""updateCount"":0,""deleteCount"":1,""replaceCount"":0}"
        final int recordKeyCount = record.length;
        final int group = recordKeyCount / 3;
        //LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        final int fieldsCount = tapTable.size();

        final int cdcTypeIndex = (group - 1) * 3;
        final String cdcType = fieldsCount < group ? record[cdcTypeIndex] : INSERT;
        String cdcInfoStr = fieldsCount < group ? record[cdcTypeIndex + 1] : null;
        Map<String, Object> cdcInfo = null;
        try {
            cdcInfo = (Map<String, Object>) fromJson(cdcInfoStr);
        } catch (Exception e) {
            cdcInfo = new HashMap<>();
        }
        if (null == cdcInfo) cdcInfo = new HashMap<>();

        int index = 0;
        Map<String, Object> after = new HashMap<>();
        Map<String, Object> before = new HashMap<>();
        final boolean isDel = DELETE.equals(cdcType);
        final boolean isIns = INSERT.equals(cdcType);
        for (Map.Entry<String, TableTypeEntity> fieldEntry : tapTable.entrySet()) {
            final String fieldName = fieldEntry.getKey();
            final TableTypeEntity typeEntity = fieldEntry.getValue();
            final String sybaseType = typeEntity.getType();

            //ignore timestamp
            if (null != sybaseType && sybaseType.toUpperCase().startsWith("TIMESTAMP")) {
                index++;
                continue;
            }

            int fieldValueIndex = index * 3;
            if (!isDel) {
                final Object value = recordKeyCount <= fieldValueIndex ? null : record[fieldValueIndex];
                after.put(fieldName, DEFAULT_CONVERT.convert(value, sybaseType, typeEntity.getTypeNum(), config, nodeConfig));
            }
            if (!isIns) {
                int fieldBeforeValueIndex = fieldValueIndex + 1;
                final Object beforeValue = recordKeyCount <= fieldBeforeValueIndex ? null : record[fieldBeforeValueIndex];
                before.put(fieldName, DEFAULT_CONVERT.convert(beforeValue, sybaseType, typeEntity.getTypeNum(), config, nodeConfig));
            }
            index++;
        }

        if (null != filter) {
            filter.filter(before, after, cdcInfo);
        }

        Object timestamp = cdcInfo.get("timestamp");
        long cdcReference = System.currentTimeMillis();
        try {
            cdcReference = Long.parseLong((String) timestamp);
        } catch (Exception ignore) { }
        switch (cdcType) {
            case INSERT:
                return TapSimplify.insertRecordEvent(after, tableId).referenceTime(cdcReference);
            case DELETE:
                return TapSimplify.deleteDMLEvent(before, tableId).referenceTime(cdcReference);
            default:
                return TapSimplify.updateDMLEvent(before, after, tableId).referenceTime(cdcReference);
        }
    }
}
