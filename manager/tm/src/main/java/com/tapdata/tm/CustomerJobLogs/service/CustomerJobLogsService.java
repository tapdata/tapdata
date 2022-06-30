package com.tapdata.tm.CustomerJobLogs.service;

import cn.hutool.core.date.DateTime;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.dto.CustomerJobLogsDto;
import com.tapdata.tm.CustomerJobLogs.dto.CustomerLogsLevel;
import com.tapdata.tm.CustomerJobLogs.entity.CustomerJobLogsEntity;
import com.tapdata.tm.CustomerJobLogs.repository.CustomerJobLogsRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.WebUtils;
import io.tapdata.common.logging.error.ErrorCode;
import io.tapdata.common.logging.error.ErrorCodeEnum;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;
import io.tapdata.common.logging.format.*;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.*;

@Service
@Slf4j
public class CustomerJobLogsService extends BaseService<CustomerJobLogsDto, CustomerJobLogsEntity, ObjectId, CustomerJobLogsRepository> {


    public CustomerJobLogsService(@NonNull CustomerJobLogsRepository repository) {
        super(repository, CustomerJobLogsDto.class, CustomerJobLogsEntity.class);
    }

    @Override
    public Page<CustomerJobLogsDto> find(Filter filter, UserDetail userDetail) {
        Page<CustomerJobLogsDto> DtoPage = super.find(filter, userDetail);
        Locale locale = null;
        try {
            RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
            if (requestAttributes != null){
                locale = WebUtils.getLocale(((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest());
            }
        }catch (Exception ignored){

        }
        if (locale == null){
            locale = Locale.getDefault();
        }
        String TMLocale;
        String a = locale.toString();
        switch (a) {
            case "zh_CN":
                TMLocale = "zh-cn";
                break;
            case "zh_TW":
                TMLocale = "zh-tw";
                break;
            default:
                TMLocale = "en";//throw new InvalidParameterException("un-supported locale setting for error code solutions.1");
        }
        List<CustomerJobLogsDto> items = DtoPage.getItems();
        if (CollectionUtils.isNotEmpty(items)){
            items.forEach(
                    row -> {
                        CustomerMessage customerMessage = null;
                        if(row.getKey().startsWith("agent.")) {
                            customerMessage = CustomerMessageFactory.getCustomerMessages("agent",1);
                        }else if(row.getKey().startsWith("tm.")){
                            customerMessage = CustomerMessageFactory.getCustomerMessages("tm",1);
                        }
                        String template = "";
                        if(customerMessage != null){
                            String key = row.getKey();
                            CustomerLogFormat clf = customerMessage.getCustomerLogFormat(key, TMLocale);
                            if(clf != null){
                                template = customerMessage.getCustomerLogFormat(key, TMLocale).getMessage();
                            }
                        }
                        row.setTemplate(template);
                        if (row.getParams() != null && row.getParams().get("errorCode") != null) {
                            Object errorCode1 = row.getParams().get("errorCode");
                            String errorMessage = "";
                            if (errorCode1 instanceof Integer) {
                                Integer errorCode = (Integer) row.getParams().get("errorCode");
                                if (customerMessage != null) {
                                    ErrorCode errorCodeSolutions = customerMessage.getErrorCodeSolutions(errorCode, TMLocale);
                                    if (errorCodeSolutions != null) {
                                        errorMessage = errorCodeSolutions.getMessage();
                                    }
                                    ErrorCodeEnum isDatasourceError = ErrorCodeEnum.fromErrorCode(errorCode);

                                    if (isDatasourceError != null && isDatasourceError.isDatasourceError()) {
                                        String datasource = (String) row.getParams().get("datasource");
                                        if (datasource != null) {
                                            DataSourceErrorLink errorLink = CustomerMessageFactory.getDataSourceErrorLink((String) row.getParams().get("datasource"));
                                            if (errorLink != null) {
                                                row.setLink(errorLink.getLink());
                                            }
                                        }
                                    }
                                }
                            } else if (errorCode1 instanceof String){
                                errorMessage = (String) errorCode1;
                            }
                            row.getParams().append("errorMessage",errorMessage);
                        }
                        if(row.getParams() != null && row.getParams().get("dataFlowType") != null && row.getParams().get("dataFlowType").equals(DataFlowType.clone.v)){
                            switch (TMLocale) {
                                case "zh-cn":
                                    row.getParams().append("dataFlowType",ZH_CNEnum.clusterClone.v);
                                    break;
                                case "zh-tw":
                                    row.getParams().append("dataFlowType",ZH_TWEnum.clusterClone.v);
                                    break;
                                default:
                                    row.getParams().append("dataFlowType",ENEnum.clusterClone.v);
                            }
                        } else if (row.getParams() != null && row.getParams().get("dataFlowType") != null && row.getParams().get("dataFlowType").equals(DataFlowType.sync.v)){
                            switch (TMLocale) {
                                case "zh-cn":
                                    row.getParams().append("dataFlowType",ZH_CNEnum.custom.v);
                                    break;
                                case "zh-tw":
                                    row.getParams().append("dataFlowType",ZH_TWEnum.custom.v);
                                    break;
                                default:
                                    row.getParams().append("dataFlowType",ENEnum.custom.v);
                            }
                        }
                        if(row.getLevel() != null){
                            row.getParams().append("level",getFormat(TMLocale,row.getLevel().toString()));
                        }
                        row.setSearchKey(null);
                        row.setCustomId(null);
                        row.setKey(null);
                        row.setVersion(null);
                        row.setUserId(null);
                        row.setCreateAt(null);
                    }
            );
        }
        return DtoPage;
    }

    @Override
    protected void beforeSave(CustomerJobLogsDto dto, UserDetail userDetail) {
        BasicBSONObject params = dto.getParams();
        StringBuilder searchKey = new StringBuilder();
        for (Map.Entry<String, Object> stringObjectEntry : params.entrySet()) {
            searchKey.append(stringObjectEntry.getValue()).append("\n");
        }
        dto.setSearchKey(searchKey.toString());
    }

    public void TMCustomerLogger(CustomerJobLogsDto customerJobLogs,UserDetail userDetail) throws BizException {
        customerJobLogs.setTimestamp(new DateTime().getTime());
        customerJobLogs.setDate(new DateTime());
        customerJobLogs.setVersion(1);
        this.save(customerJobLogs,userDetail);
    }

    public void startDataFlow(CustomerJobLog customerJobLog, UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_START_DATA_FLOW.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        }catch (Exception e){
            log.warn(e.getMessage());
        }
    }

    public void splittedJobs(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_SPLIT_JOBS.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("jobInfos", customerJobLog.getJobInfos());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void stopDataFlow(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_STOP_DATA_FLOW.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void forceStopDataFlow(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_FORCE_STOP_DATA_FLOW.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void resetDataFlow(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_RESET_DATA_FLOW.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void agentDown(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_AGENT_DOWN.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.ERROR);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("errorCode", ErrorCodeEnum.TM_AGENT_DOWN);
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void assignAgent(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("agentHost", customerJobLog.getAgentHost());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            if(customerJobLog.getJobName() != null){
                customerJobLogs.setKey(CustomerLogMessagesEnum.TM_ASSIGN_JOB_AGENT.getKey());
                params.append("jobName", customerJobLog.getJobName());
            }else {
                customerJobLogs.setKey(CustomerLogMessagesEnum.TM_ASSIGN_AGENT.getKey());
            }
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void noAvailableAgents(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_NO_AVAILABLE_AGENT.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.FATAL);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("errorCode", ErrorCodeEnum.TM_NO_AVAILABLE_AGENT);
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void resetJob(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_RESET_JOB.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void startJob(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_START_JOB.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.INFO);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("jobName", customerJobLog.getJobName());
            params.append("agentHost", customerJobLog.getAgentHost());
            params.append("jobInfo", customerJobLog.getJobInfos());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void stopJob(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_STOP_JOB.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.WARN);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("jobName", customerJobLog.getJobName());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void forceStopJob(CustomerJobLog customerJobLog,UserDetail userDetail) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setKey(CustomerLogMessagesEnum.TM_FORCE_STOP_JOB.getKey());
            customerJobLogs.setLevel(CustomerLogsLevel.WARN);
            BasicBSONObject params = new BasicBSONObject();
            params.append("dataFlowName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("dataFlowType", customerJobLog.getDataFlowType());
            params.append("jobName", customerJobLog.getJobName());
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

    public void error(CustomerJobLog customerJobLog,UserDetail userDetail,String errorMessage,String errorCode) throws BizException {
        try {
            CustomerJobLogsDto customerJobLogs = new CustomerJobLogsDto();
            customerJobLogs.setDataFlowId(customerJobLog.getId());
            customerJobLogs.setLevel(CustomerLogsLevel.ERROR);
            BasicBSONObject params = new BasicBSONObject();
            params.append("jobName", customerJobLog.getName());
            params.append("dataFlowId", customerJobLog.getId());
            params.append("errorMessage", errorMessage);
            params.append("errorCode", errorCode);
            if(customerJobLog.getJobName() != null) {
                customerJobLogs.setKey(CustomerLogMessagesEnum.TM_JOB_ERROR.getKey());
                params.append("jobName", customerJobLog.getJobName());
            }else{
                customerJobLogs.setKey(CustomerLogMessagesEnum.TM_ERROR.getKey());
            }
            customerJobLogs.setParams(params);
            TMCustomerLogger(customerJobLogs, userDetail);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }


    public enum DataFlowType {
        clone("cluster-clone"),
        sync("custom"),
        ;

        @Getter
        final String v;
        DataFlowType(String v) {
            this.v = v;
        }
    }

    private String getFormat(String TMLocale,String Key){
        switch (TMLocale) {
            case "zh-cn":
                return ZH_CNEnum.valueOf(Key).v;
            case "zh-tw":
                return ZH_TWEnum.valueOf(Key).v;
            default:
                return ENEnum.valueOf(Key).v;
        }
    }

    public enum ZH_CNEnum {
        clusterClone("迁移任务"),
        custom("同步任务"),
        INFO("信息"),
        WARN("警告"),
        ERROR("错误"),
        FATAL("致命错误"),
        ;

        @Getter
        final String v;
        ZH_CNEnum(String v) {
            this.v = v;
        }
    }

    public enum ZH_TWEnum {
        clusterClone("遷移任務"),
        custom("同步任務"),
        INFO("信息"),
        WARN("警告"),
        ERROR("錯誤"),
        FATAL("致命錯誤"),
        ;

        @Getter
        final String v;
        ZH_TWEnum(String v) {
            this.v = v;
        }
    }

    public enum ENEnum {
        clusterClone("Database Migration"),
        custom("Data Sync"),
        INFO("INFO"),
        WARN("WARN"),
        ERROR("ERROR"),
        FATAL("FATAL"),
        ;

        @Getter
        final String v;
        ENEnum(String v) {
            this.v = v;
        }
    }
}
