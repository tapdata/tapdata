package com.tapdata.tm.schedule.transform;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.NodeEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

/**
 * @Author: Gavin'Xiao
 * @Date: 2024/5/8 23:01
 * @Description:
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TransformSchemaServiceImpl {
    private TaskService taskService;
    private TransformSchemaService transformSchemaService;

    public void transformSchema(TaskDto taskDto, UserDetail user) {
        DAG dag = taskDto.getDag();
        transformSchemaService.transformSchema(dag, user, taskDto.getId());
        //暂时先这样子更新，为了保存join节点中的推演中产生的primarykeys数据
        long count = dag.getNodes().stream()
                .filter(n -> NodeEnum.join_processor.name().equals(n.getType()))
                .count();
        if (count != 0) {
            Update update = new Update();
            update.set("dag", taskDto.getDag());
            taskService.updateById(taskDto.getId(), update, user);
        }
    }
}
