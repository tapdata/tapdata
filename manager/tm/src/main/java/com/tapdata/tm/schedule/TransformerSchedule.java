package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.TransformWsResp;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.commons.util.ThrowableUtils;
import com.tapdata.tm.ws.handler.TransformerStatusPushHandler;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
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

    private TransformSchemaService transformSchemaService;
    private TaskService taskService;

    private UserService userService;

    /**
     * 每五秒执行一次，发送推演消息记录
     */
    @Scheduled(fixedDelay = 5 * 1000)
    @SchedulerLock(name ="task_transform_retry", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void transformer() {
        ws();
        error();
        completeJsTransformer();
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
            log.error("TransformerSchedule error {}", ThrowableUtils.getStackTraceByPn(e));
        }
    }

    public void ws() {
        log.debug("start transform ws message push.");
        //获取当前节点监听的任务id
        Set<String> taskIds = TransformerStatusPushHandler.transformMap.keySet();
        if (CollectionUtils.isEmpty(taskIds)) {
            return;
        }
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


    //对于js节点推演总是存在一些一直推演中的问题，可能是引擎连接断了，于是加上兜底方案，如果在较长一段时间后，还是推演失败，就走tm推演。
    public void completeJsTransformer() {

        try {
            //查询较长时间没有推演结束的任务，较长时间定义为30秒。   private String transformUuid;
            //    private Boolean transformed;
            long currentTime = System.currentTimeMillis();
            String currentTimeString = String.valueOf(currentTime - 30000);
            Criteria criteria = Criteria.where("transformed").ne(true)
                    .and("transformUuid").lt(currentTimeString)
                    .and("is_deleted").ne(true)
                    .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                    .and("dag.nodes.type").in("migrate_js_processor", "js_processor");

            Query query = new Query(criteria);
            List<TaskDto> taskDtos = taskService.findAll(query);
            if (CollectionUtils.isEmpty(taskDtos)) {
                return;
            }

            List<TaskDto> retryTasks = new ArrayList<>();
            for (TaskDto taskDto : taskDtos) {
                //对于数据复制，100个表20秒的返回时间计算。
                if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                    LinkedList<DatabaseNode> sourceNode = taskDto.getDag().getSourceNode();
                    if (CollectionUtils.isEmpty(sourceNode)) {
                        return;
                    }

                    DatabaseNode databaseNode = sourceNode.get(0);
                    int i = databaseNode.tableSize();
                    int needTimes = 200 * i;
                    String transformUuid = taskDto.getTransformUuid();
                    long startTime = Long.parseLong(transformUuid);
                    if ((startTime + needTimes) < currentTime) {
                        retryTasks.add(taskDto);
                    }

                } else {
                    //对于数据开发的任务，30秒基本可以走兜底方案了。
                    retryTasks.add(taskDto);
                }

            }


            for (TaskDto taskDto : retryTasks) {
                try {
                    UserDetail user = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
                    transformSchemaService.transformSchema(taskDto, user, false);
                } catch (Exception e) {
                    log.info("task transform retry error, task name = {}", taskDto.getName());
                }
            }
        } catch (Exception e) {
            log.warn("task transform retry error!");
        }
    }


}
