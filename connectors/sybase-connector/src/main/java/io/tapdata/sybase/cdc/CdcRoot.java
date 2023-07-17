package io.tapdata.sybase.cdc;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * @author GavinXiao
 * @description CdcRoot create by Gavin
 * @create 2023/7/13 11:09
 **/
public class CdcRoot {
    private String cdcId;
    private File cdcFile;
    private Process process;
    private TapConnectorContext context;
    private String sybasePocPath;
    private CdcStartVariables variables;
    private List<String> cdcTables;
    private String cliPath;
    private Predicate<Void> isAlive;

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

    public void setSybasePocPath(String sybasePocPath) {
        this.sybasePocPath = sybasePocPath;
    }

    public CdcStartVariables getVariables() {
        return variables;
    }

    public void setVariables(CdcStartVariables variables) {
        this.variables = variables;
    }

    public List<String> getCdcTables() {
        return cdcTables;
    }

    public void setCdcTables(List<String> cdcTables) {
        this.cdcTables = cdcTables;
    }

    public void addCdcTable(String tableId) {
        if (null == tableId || "".equals(tableId.trim())) return;
        if (null == cdcTables) {
            cdcTables = new ArrayList<>();
            cdcTables.add(tableId);
        } else {
            if (!cdcTables.contains(tableId)) {
                cdcTables.add(tableId);
            }
        }
    }

    public String getCdcId() {
        return cdcId;
    }

    public void setCdcId(String cdcId) {
        this.cdcId = cdcId;
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
}
