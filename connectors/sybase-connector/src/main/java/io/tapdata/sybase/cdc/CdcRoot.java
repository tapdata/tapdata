package io.tapdata.sybase.cdc;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.sybase.cdc.dto.start.CdcStartVariables;

import java.io.File;
import java.util.List;

/**
 * @author GavinXiao
 * @description CdcRoot create by Gavin
 * @create 2023/7/13 11:09
 **/
public class CdcRoot {
    String cdcId;
    File cdcFile;
    Process process;
    TapConnectorContext context;
    String sybasePocPath;
    CdcStartVariables variables;
    List<String> cdcTables;


    public File getCdcFile() {
        return cdcFile;
    }

    public void setCdcFile(File cdcFile) {
        this.cdcFile = cdcFile;
    }


    public boolean checkStep(){

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

    public String getCdcId() {
        return cdcId;
    }

    public void setCdcId(String cdcId) {
        this.cdcId = cdcId;
    }
}
