package io.tapdata.sybase.cdc;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;
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

    public CdcRoot(Predicate<Void> isAlive) {
        this.isAlive = isAlive;
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
        return cliPath;
    }

    public void setCliPath(String cliPath) {
        if (null == cliPath) return;
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
