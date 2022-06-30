package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.SubTaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author: Zed
 * @Date: 2022/2/9
 * @Description:
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class SubTaskRestartSchedule {

    private SubTaskService subTaskService;
    private UserService userService;

    /**
     * 定时重启子任务，只要找到有重启标记，并且是停止状态的任务，就重启，每分钟启动一次
     */
    @Scheduled(fixedDelay = 10 * 1000)
    @SchedulerLock(name ="restart_subtask_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void restartSubTask() {
        //查询到所有需要重启的子任务
        Criteria criteria = Criteria.where("restartFlag").is(true).and("status").is("stop");
        Query query = new Query(criteria);
        query.fields().include("_id", "restartUserId");
        List<SubTaskDto> restartSubTasks = subTaskService.findAll(query);
        for (SubTaskDto subTask : restartSubTasks) {

            try {
                UserDetail user = userService.loadUserById(MongoUtils.toObjectId(subTask.getRestartUserId()));
                subTaskService.start(subTask.getId(), user);
            } catch (Exception e) {
                log.warn("restart subtask error, subtask id = {}, e = {}", subTask.getId(), e.getMessage());
            }
        }
    }
}
