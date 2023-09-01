package io.tapdata.sybase.cdc;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.read.CdcPosition;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
import io.tapdata.sybase.cdc.service.ListenFile;
import io.tapdata.sybase.util.ConnectorUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * @author GavinXiao
 * @description CdcRoot create by Gavin
 * @create 2023/7/13 11:09
 **/
public class CdcRoot {
    private String taskCdcId;
    private File cdcFile;
    private Process process;
    private TapConnectorContext context;
    private String sybasePocPath;
    private CdcStartVariables variables;
    private Map<String, Map<String, List<String>>> cdcTables;
    private String cliPath;
    private Predicate<Void> isAlive;
    private Map<String, Map<String, List<String>>> containsTimestampFieldTables;
    Map<String, String> csvFileModifyIndexCache;


    public CdcRoot(Predicate<Void> isAlive) {
        this.isAlive = isAlive;
    }

    public synchronized Integer getCsvFileModifyIndexByCsvFileName(String csvFileName) {
//        if (null == csvFileModifyIndexCache || csvFileModifyIndexCache.isEmpty()) return 0;
//        String indexAndLine = csvFileModifyIndexCache.get(ListenFile.databaseTable(csvFileName));
//        if (null == indexAndLine) return 0;
//        return Optional.ofNullable(stringIntegerMap.get("index")).orElse(0);
        Integer[] indexAndLine = indexAndLineFromCsvFileModifyIndexCache(csvFileName);
        return indexAndLine[0];
    }

    private Integer[] indexAndLineFromCsvFileModifyIndexCache(String csvFileName) {
        Integer[] indexAndLine = {0, 0};
        if (null == csvFileModifyIndexCache || csvFileModifyIndexCache.isEmpty()) return indexAndLine;
        String indexAndLineString = csvFileModifyIndexCache.get(ListenFile.databaseTable(csvFileName));
        if (null == indexAndLineString) return indexAndLine;
        String[] split = indexAndLineString.split(",");
        try {
            switch (split.length) {
                case 0:
                    return indexAndLine;
                case 1:
                    indexAndLine[0] = Integer.parseInt(split[0]);
                default:
                    indexAndLine[0] = Integer.parseInt(split[0]);
                    indexAndLine[1] = Integer.parseInt(split[1]);
            }
        } catch (Exception ignore) {}
        return indexAndLine;
    }

    public synchronized Integer getCsvFileModifyLineByCsvFileName(String csvFileName) {
//        if (null == csvFileModifyIndexCache || csvFileModifyIndexCache.isEmpty()) return 0;
//        String stringIntegerMap = csvFileModifyIndexCache.get(ListenFile.databaseTable(csvFileName));
//        if (null == stringIntegerMap) return 0;
//        Integer index = CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(csvFileName);
//        Integer lastIndex = stringIntegerMap.get("index");
//        return null == lastIndex || !lastIndex.equals(index) ? 0 : Optional.ofNullable(stringIntegerMap.get("line")).orElse(0);
        Integer[] indexAndLine = indexAndLineFromCsvFileModifyIndexCache(csvFileName);
        return indexAndLine[1];
    }

    public synchronized Map<String, String> setCsvFileModifyIndexByCsvFileName(String csvFileName, int index, int acceptLine) {
        return setCsvFileModifyIndexByFullTableName(ListenFile.databaseTable(csvFileName), index, acceptLine);
    }

    /**
     * @param fullTableName {databaseName}.{schemaName}.{tableName}
     * */
    public synchronized Map<String, String> setCsvFileModifyIndexByFullTableName(String fullTableName, int index, int acceptLine) {
        if (null == csvFileModifyIndexCache) {
            csvFileModifyIndexCache = new HashMap<>();
        }
//        Map<String, Integer> offset =  csvFileModifyIndexCache.computeIfAbsent(fullTableName, key -> new HashMap<>());
//        offset.put("index", index);
//        offset.put("line", acceptLine);
        csvFileModifyIndexCache.put(fullTableName, String.format("%s,%s", index, acceptLine));
        return csvFileModifyIndexCache;
    }

    public CdcRoot csvFileModifyIndexCache(Object csvFileModifyIndexCache) {
        try {
            this.csvFileModifyIndexCache = (Map<String, String>)csvFileModifyIndexCache;
        } catch (Exception e){
            this.csvFileModifyIndexCache = new HashMap<>();
            if (csvFileModifyIndexCache instanceof Map) {
                Map<String, Object> cache = (Map<String, Object>) csvFileModifyIndexCache;
                cache.forEach((k,v) -> {
                    if(v instanceof Map) {
                        Map<String, Object> map = (Map<String, Object>) v;
                        this.csvFileModifyIndexCache.put(k, String.format("%s,%s",
                                Integer.parseInt(String.valueOf(Optional.ofNullable(map.get("index")).orElse(0))),
                                Integer.parseInt(String.valueOf(Optional.ofNullable(map.get("line")).orElse(0)))));
                    }
                });
            }
        }
        return this;
    }

    public File getCdcFile() {
        return cdcFile;
    }

    public void setCdcFile(File cdcFile) {
        this.cdcFile = cdcFile;
    }


    public boolean checkStep() {

        return true;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public TapConnectorContext getContext() {
        return context;
    }

    public void setContext(TapConnectorContext context) {
        this.context = context;
    }

    public String getSybasePocPath() {
        return sybasePocPath.endsWith("/") ? sybasePocPath.substring(0, sybasePocPath.length() - 1) : sybasePocPath;
    }

    public static final String POC_TEMP_CONFIG_PATH = "sybase-poc-temp/%s/sybase-poc/config/sybase2csv/filter_sybasease.yaml";
    public static final String POC_TEMP_TRACE_LOG_PATH = "sybase-poc-temp/%s/sybase-poc/config/sybase2csv/trace/%s/trace.log";


    public String getFilterTableConfigPath() {
        return new File(String.format(POC_TEMP_CONFIG_PATH, ConnectorUtil.getCurrentInstanceHostPortFromConfig(context))).getAbsolutePath();
    }

    public void setSybasePocPath(String sybasePocPath) {
        this.sybasePocPath = sybasePocPath;
    }

    public CdcStartVariables getVariables() {
        if (null == variables) variables = new CdcStartVariables();
        return variables;
    }

    public void setVariables(CdcStartVariables variables) {
        this.variables = variables;
    }

    public Map<String, Map<String, List<String>>> getCdcTables() {
        return cdcTables;
    }

    public void setCdcTables(Map<String, Map<String, List<String>>> cdcTables) {
        this.cdcTables = cdcTables;
    }

    public String getTaskCdcId() {
        return taskCdcId;
    }

    public void setTaskCdcId(String taskCdcId) {
        this.taskCdcId = taskCdcId;
    }

    public String getCliPath() {
        return Optional.ofNullable(cliPath).orElse(new File(CdcRoot.CLI_PATH).getAbsolutePath());
    }

    public static String CLI_PATH = "sybase-poc/replicant-cli";
    public void setCliPath(String cliPath) {
        if (null == cliPath) cliPath = new File(CdcRoot.CLI_PATH).getAbsolutePath();
        this.cliPath = cliPath.endsWith("/") ? cliPath.substring(0, cliPath.length() - 1) : cliPath;
    }

    public Predicate<Void> getIsAlive() {
        return isAlive;
    }

    public void setIsAlive(Predicate<Void> isAlive) {
        this.isAlive = isAlive;
    }

    public Map<String, Map<String, List<String>>> getContainsTimestampFieldTables() {
        return containsTimestampFieldTables;
    }

    public List<String> getContainsTimestampFieldTables(String database, String schema) {
        return null == containsTimestampFieldTables ? null : Optional.ofNullable(containsTimestampFieldTables.get(database)).orElse(new HashMap<String, List<String>>()).get(schema);
    }

    public boolean hasContainsTimestampFieldTables(String database, String schema, String tableName) {
        List<String> tables = getContainsTimestampFieldTables(database, schema);
        return null != tables && !tables.isEmpty() && tables.contains(tableName);
    }

    public void setContainsTimestampFieldTables(Map<String, Map<String, List<String>>> containsTimestampFieldTables) {
        this.containsTimestampFieldTables = containsTimestampFieldTables;
    }
}
