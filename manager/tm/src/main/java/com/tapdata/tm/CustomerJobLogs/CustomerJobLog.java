package com.tapdata.tm.CustomerJobLogs;

import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import java.util.List;

/**
 * @author Steven
 */
public class CustomerJobLog {

    public CustomerJobLog(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public CustomerJobLog(String id, String name, CustomerJobLogsService.DataFlowType dataFlowType) {
        this.id = id;
        this.name = name;
        this.dataFlowType = dataFlowType.getV();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDataFlowType() {
        return dataFlowType;
    }

    public String getJobInfos() {
        return jobInfos;
    }

    public String getAgentHost() {
        return agentHost;
    }

    public String getJobName() {
        return jobName;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDataFlowType(String dataFlowType) {
        this.dataFlowType = dataFlowType;
    }

    public void setJobInfos(String jobInfos) {
        this.jobInfos = jobInfos;
    }

    public void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    private String id;
    private String name;
    private String dataFlowType;
    private String jobInfos;
    private String agentHost;
    private String jobName;
}
