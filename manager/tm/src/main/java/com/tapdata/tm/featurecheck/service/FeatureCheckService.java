package com.tapdata.tm.featurecheck.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.featurecheck.dto.AgentDto;
import com.tapdata.tm.featurecheck.dto.FeatureCheckDto;
import com.tapdata.tm.featurecheck.dto.FeatureCheckResult;
import com.tapdata.tm.featurecheck.entity.FeatureCheckEntity;
import com.tapdata.tm.featurecheck.repository.FeatureCheckRepository;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class FeatureCheckService extends BaseService<FeatureCheckDto, FeatureCheckEntity, ObjectId, FeatureCheckRepository> {
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)");
    @Autowired
    WorkerService workerService;

    @Autowired
    TaskService taskService;



    public FeatureCheckService(@NonNull FeatureCheckRepository repository) {
        super(repository, FeatureCheckDto.class, FeatureCheckEntity.class);
    }

    @Override
    protected void beforeSave(FeatureCheckDto dto, UserDetail userDetail) {

    }


    public FeatureCheckResult queryFeatureCheck(List<FeatureCheckDto> featureCheckDtoList, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(featureCheckDtoList)) {
            log.error("FeatureCheck param is empty");
            throw new BizException("featureCheck.param.empty");
        }
        List<Worker> workers = workerService.findAvailableAgent(userDetail);
        FeatureCheckResult featureCheckResult = new FeatureCheckResult();
        List<AgentDto> agents = new ArrayList<>();
        List<String> eligibleAgents = new ArrayList<>();
        List<FeatureCheckDto> featureCheckDtoTemp = findAll(new Query());
        for (Worker worker : workers) {
            AgentDto agentDto = new AgentDto();
            WorkerDto workerDto = new WorkerDto();
            BeanUtils.copyProperties(worker, workerDto);
            int limitTaskNum = workerService.getLimitTaskNum(workerDto, userDetail);
            agentDto.setId(worker.getId().toHexString());
            agentDto.setVersion(worker.getVersion());
            agentDto.setLimitTasks(limitTaskNum);
            String processId = worker.getProcessId();
            int runningNum = taskService.runningTaskNum(processId, userDetail);
            agentDto.setRunningTasks(runningNum);
            agents.add(agentDto);

            boolean eligibleAgent = true;
            String agentVersion = StringUtils.isNotBlank(worker.getVersion()) ? worker.getVersion() : worker.getTcmInfo().getVersion();
            for (FeatureCheckDto featureCheckDto : featureCheckDtoList) {
                for (FeatureCheckDto featureCheck : featureCheckDtoTemp) {
                    if (featureCheckDto.getFeatureType().equals(featureCheck.getFeatureType()) &&
                            featureCheckDto.getFeatureCode().equals(featureCheck.getFeatureCode())) {
                        featureCheckDto.setMinAgentVersion(featureCheck.getMinAgentVersion());
                        featureCheckDto.setDescription(featureCheck.getDescription());
                        int compare = compareVersion(agentVersion, featureCheck.getMinAgentVersion());
                        if (compare < 0) {
                            eligibleAgent = false;
                        } else {
                            if (CollectionUtils.isNotEmpty(featureCheckDto.getSupportedAgents())) {
                                featureCheckDto.getSupportedAgents().add(worker.getId().toHexString());
                            } else {
                                List<String> supportedAgents = new ArrayList<>();
                                supportedAgents.add(worker.getId().toHexString());
                                featureCheckDto.setSupportedAgents(supportedAgents);
                            }
                        }
                    }
                }
            }
            if (eligibleAgent) {
                eligibleAgents.add(worker.getId().toHexString());
            }
        }
        featureCheckResult.setAgents(agents);
        featureCheckResult.setResult(featureCheckDtoList);
        featureCheckResult.setEligibleAgents(eligibleAgents);
        return featureCheckResult;
    }


    public static int compareVersion(String engineVersion, String featureVersion) {
        engineVersion =engineVersion.contains("-")?engineVersion.substring(0, engineVersion.indexOf("-")):engineVersion;
        featureVersion =featureVersion.contains("-")?featureVersion.substring(0, featureVersion.indexOf("-")):featureVersion;

        Matcher engineVersionMatcher = VERSION_PATTERN.matcher(engineVersion);
        Matcher featureVersionMatcher = VERSION_PATTERN.matcher(featureVersion);
        int result = 0;

        while (engineVersionMatcher.find() && featureVersionMatcher.find()) {
            int v1 = Integer.parseInt(engineVersionMatcher.group());
            int v2 = Integer.parseInt(featureVersionMatcher.group());

            if (v1 < v2) {
                result = -1;
                break;
            } else if (v1 > v2) {
                result = 1;
                break;
            }
        }

        if (engineVersionMatcher.find() && !featureVersionMatcher.find()) {
            result = 1;
        } else if (!engineVersionMatcher.find() && featureVersionMatcher.find()) {
            result = -1;
        }

        return result;
    }
}
