package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.TransformWsResp;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.ws.handler.TransformerStatusPushHandler;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/3/5
 * @Description:
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TransformerSchedule {

    private MetadataTransformerService transformerService;
    private TaskService taskService;

    /**
     * 每五秒执行一次，发送推演消息记录
     */
    @Scheduled(fixedDelay = 5 * 1000)
    public void transformer() {
        ws();
        error();
    }

    public void error() {
        try {
            //获取当前节点监听的任务id
            log.debug("start check transform error list");
            Criteria criteria = Criteria.where("status").is(MetadataTransformerDto.StatusEnum.running.name()).and("pingTime").gte(System.currentTimeMillis() - (12 * 1000));
            Query query = new Query(criteria);

            Update update = new Update().set("status", MetadataTransformerDto.StatusEnum.error.name());
            transformerService.update(query, update);
        } catch (Exception e) {
            log.error("TransformerSchedule error", e);
        }
    }

    public void ws() {
        log.debug("start transform ws message push.");
        //获取当前节点监听的任务id
        Set<String> taskIds = TransformerStatusPushHandler.transformMap.keySet();
        //查询所有的当前监听的，并且在推演过程中的任务

        Criteria criteria = Criteria.where("dataFlowId").in(taskIds).and("pingTime").gte(System.currentTimeMillis() - (8 *1000));
        Query query = new Query(criteria);
        List<MetadataTransformerDto> transformers = transformerService.findAll(query);

        List<TaskDto> taskEntityList = taskService.findAllTasksByIds(new ArrayList<>(taskIds));
        Map<String, TaskDto> taskMap = taskEntityList.stream().collect(Collectors.toMap(t->t.getId().toHexString(), n -> n));


        log.debug("find transform record num = {}", transformers.size());
        for (MetadataTransformerDto transformer : transformers) {
            try {
                String dataFlowId = transformer.getDataFlowId();
                TaskDto taskDto = taskMap.get(dataFlowId);
                String sinkNodeId = transformer.getStageId();

                DatabaseNode databaseNode = taskDto.getDag().getTargets().stream()
                        .filter(node -> sinkNodeId.equals(node.getId())).map(m -> (DatabaseNode) m).findFirst().orElse(null);

                Assert.notNull(databaseNode, "databaseNode must be not null!");
                if (!Objects.isNull(databaseNode.getSyncObjects())) {
                    List<String> objectNames = databaseNode.getSyncObjects().get(0).getObjectNames();
                    if(objectNames.size() != transformer.getTotal()) {
                        continue;
                    }
                }

                long useTime = System.currentTimeMillis() - transformer.getBeginTimestamp();
                TransformWsResp build = TransformWsResp.builder()
                        .progress(transformer.getFinished() / (transformer.getTotal() * 1d))
                        .total(transformer.getTotal())
                        .finished(transformer.getFinished())
                        .status(transformer.getStatus())
                        .stageId(transformer.getStageId())
                        .build();

                int remainingTime = (int) (useTime / (1 - build.getProgress()));
                build.setRemainingTime(remainingTime);
                build.setProgress(((int) (build.getProgress() * 100)) / 100d);

                TransformerStatusPushHandler.sendTransformMessage(dataFlowId, build);
            } catch (Exception e) {
                log.error("TransformerSchedule ws error", e);
            }
        }
    }


}
