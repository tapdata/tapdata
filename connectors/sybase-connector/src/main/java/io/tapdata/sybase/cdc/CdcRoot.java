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
    Map<String, Map<String, Integer>> csvFileModifyIndexCache;


    public CdcRoot(Predicate<Void> isAlive) {
        this.isAlive = isAlive;
    }
    public synchronized Integer getCsvFileModifyIndexByCsvFileName(String csvFileName) {
        try {
            if (null != this.csvFileModifyIndexCache && !this.csvFileModifyIndexCache.isEmpty()) {
                Map<String, Integer> stringIntegerMap = (Map)this.csvFileModifyIndexCache.get(ListenFile.databaseTable(csvFileName));
                return null != stringIntegerMap && !stringIntegerMap.isEmpty() ? (Integer)Optional.ofNullable(stringIntegerMap.get("index")).orElse(0) : 0;
            } else {
                return 0;
            }
        } catch (Exception e) {
            return  0;
        }
    }
    public synchronized Integer getCsvFileModifyLineByCsvFileName(String csvFileName) {
        try {
            if (null != this.csvFileModifyIndexCache && !this.csvFileModifyIndexCache.isEmpty()) {
                Map<String, Integer> stringIntegerMap = this.csvFileModifyIndexCache.get(ListenFile.databaseTable(csvFileName));
                if (null == stringIntegerMap) {
                    return 0;
                } else {
                    Integer index = CdcPosition.PositionOffset.fixFileNameByFilePathWithoutSuf(csvFileName);
                    Integer lastIndex = stringIntegerMap.get("index");
                    return null != lastIndex && lastIndex.equals(index) ? Optional.ofNullable(stringIntegerMap.get("line")).orElse(0) : 0;
                }
            } else {
                return 0;
            }
        } catch (Exception e) {
            return 0;
        }
    }

    public synchronized Map<String, Map<String, Integer>> setCsvFileModifyIndexByCsvFileName(String csvFileName, int index, int acceptLine) {
        return this.setCsvFileModifyIndexByFullTableName(ListenFile.databaseTable(csvFileName), index, acceptLine);
    }

    public synchronized Map<String, Map<String, Integer>> setCsvFileModifyIndexByFullTableName(String fullTableName, int index, int acceptLine) {
        if (null == this.csvFileModifyIndexCache) {
            this.csvFileModifyIndexCache = new HashMap();
        }
        Map<String, Integer> offset = (Map<String, Integer>)this.csvFileModifyIndexCache.computeIfAbsent(fullTableName, (key) -> {
            return new HashMap();
        });
        offset.put("index", index);
        offset.put("line", acceptLine);
        return this.csvFileModifyIndexCache;
    }

    public CdcRoot csvFileModifyIndexCache(Map<String, Map<String, Integer>> csvFileModifyIndexCache) {
        this.csvFileModifyIndexCache = csvFileModifyIndexCache;
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