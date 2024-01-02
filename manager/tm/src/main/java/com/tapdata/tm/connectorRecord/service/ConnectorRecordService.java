package com.tapdata.tm.connectorRecord.service;


import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.repository.ConnectorRecordRepository;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import com.tapdata.tm.ws.dto.MessageInfo;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class ConnectorRecordService extends BaseService<ConnectorRecordDto, ConnectorRecordEntity, ObjectId, ConnectorRecordRepository> {
    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private WorkerService workerService;

    @Autowired
    private MessageQueueService messageQueueService;

    public ConnectorRecordService(@NonNull ConnectorRecordRepository repository) {
        super(repository, ConnectorRecordDto.class, ConnectorRecordEntity.class);
    }

    @Override
    protected void beforeSave(ConnectorRecordDto dto, UserDetail userDetail) {
        // TODO document why this method is empty
    }

    public ConnectorRecordEntity uploadConnectorRecord(ConnectorRecordDto connectorRecordDto,UserDetail userDetail){
        Document document = mongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class,"DatabaseTypes");
        ConnectorRecordEntity connectorRecord = new ConnectorRecordEntity();
        BeanUtils.copyProperties(connectorRecordDto,connectorRecord);
        if(document != null && document.getString("pdkId") != null){
            connectorRecord.setPdkId(document.getString("pdkId"));
            return repository.insert(connectorRecord,userDetail);
        }
        return null;
    }

    public ConnectorRecordEntity queryByConnectionId(String connectionId) {
        ConnectorRecordEntity connectorRecord = mongoTemplate.findOne(Query.query(Criteria.where("connectionId").is(connectionId)), ConnectorRecordEntity.class);
        return connectorRecord;
    }

    public void deleteByConnectionId(String connectionId) {
         mongoTemplate.remove(Query.query(Criteria.where("connectionId").is(connectionId)), ConnectorRecordEntity.class);
    }

    public void sendMessage(MessageInfo messageInfo, UserDetail userDetail) {
        Map<String, Object> data = messageInfo.getData();
        data.put("type", messageInfo.getType());
        messageInfo.setType("pipe");
        List<String> tags = addAgentTags(data);
        AtomicReference<String> receiver = getReceiver(data,tags,userDetail);
        String agentId = receiver.get();
        MessageQueueDto messageQueueDto=new MessageQueueDto();
        messageQueueDto.setReceiver(agentId);
        messageQueueDto.setData(data);
        messageQueueDto.setType("pipe");
        messageQueueService.sendMessage(messageQueueDto);
    }
    public List<String> addAgentTags(Map<String, Object> data) {
        Map platformInfos = MapUtils.getAsMap(data, "platformInfo");
        List<String> tags = new ArrayList<>();
        if (MapUtils.isNotEmpty(platformInfos)){
            List<String> list = Arrays.asList("region", "zone", "agentType");
            for (Object o : platformInfos.keySet()) {
                if (list.contains(o.toString()) && platformInfos.get(o) != null){
                    tags.add(platformInfos.get(o).toString());
                }
                if (platformInfos.get(o) instanceof Boolean && (Boolean)platformInfos.get(o)){
                    tags.add("internet");
                }
            }
            data.put("agentTags", tags);
        }
        return tags;
    }

    public AtomicReference<String> getReceiver(Map<String, Object> data, List<String> tags, UserDetail userDetail) {
        AtomicReference<String> receiver = new AtomicReference<>("");
        try {
            boolean accessNodeTypeEmpty = (Boolean) data.getOrDefault("accessNodeTypeEmpty", false);
            Object accessNodeType = data.get("accessNodeType");
            FunctionUtils.isTureOrFalse(accessNodeTypeEmpty ||
                            AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name().equals(accessNodeType))
                    .trueOrFalseHandle(() -> {
                        SchedulableDto schedulableDto = new SchedulableDto();
                        schedulableDto.setAgentTags(tags);
                        schedulableDto.setUserId(userDetail.getUserId());
                        CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(schedulableDto, userDetail, "testConnection", "testConnection");
                        receiver.set(calculationEngineVo.getProcessId());
                    }, () -> {
                        Object accessNodeProcessId = data.get("accessNodeProcessId");
                        FunctionUtils.isTureOrFalse(Objects.nonNull(accessNodeProcessId)).trueOrFalseHandle(() -> {
                            String processId = accessNodeProcessId.toString();
                            receiver.set(processId);

                            List<Worker> availableAgents = workerService.findAvailableAgentByAccessNode(userDetail, Lists.newArrayList(processId));
                            if (CollectionUtils.isEmpty(availableAgents)) {
                                data.put("status", "error");
                                data.put("msg", "Worker " + processId + " not available, receiver is blank");
                            }
                        }, () -> {
                            data.put("status", "error");
                            data.put("msg", "Worker set error, receiver is blank");
                        });
                    });
        } catch (Exception e) {
            log.error("error {}", e.getMessage());
        }
        return receiver;
    }
}
