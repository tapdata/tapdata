package com.tapdata.tm.customerJobLogs;

import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class CustomerJobLogsTest extends BaseJunit {

    @Autowired
    CustomerJobLogsService customerJobLogs;

    @Test
    void customerJobLogsAgentDown() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.agentDown(customerJobLog,user);
    }

    @Test
    void customerJobLogsAssignAgent() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.assignAgent(customerJobLog,user);
    }

    @Test
    void customerJobLogsError() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.error(customerJobLog,user,"testError","91999");
    }

    @Test
    void customerJobLogsForceStopDataFlow() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.forceStopDataFlow(customerJobLog,user);
    }

    @Test
    void customerJobLogsNoAvailableAgents() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.noAvailableAgents(customerJobLog,user);
    }

    @Test
    void customerJobLogsResetDataFlow() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.resetDataFlow(customerJobLog,user);
    }

    @Test
    void customerJobLogsSplittedJobs() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.splittedJobs(customerJobLog,user);
    }

    @Test
    void customerJobLogsStartDataFlow() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.startDataFlow(customerJobLog,user);
    }

    @Test
    void customerJobLogsStopDataFlow() {
        DataFlowDto entity = new DataFlowDto();
        UserDetail user = new UserDetail("5f32027814fbc2d8530c5fe3","","admin@admin.com","admin", Collections.singleton(new SimpleGrantedAuthority("admin")));
        entity.setUserId("5f32027814fbc2d8530c5fe3");
        entity.setName("test123");
        entity.setId(new ObjectId());
        CustomerJobLog customerJobLog = new CustomerJobLog("61c004ae780a520011391762","jobName");
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogs.stopDataFlow(customerJobLog,user);
    }
}
