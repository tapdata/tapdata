package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class TapDataTaskSchedulerUtil {
    private TapDataTaskSchedulerUtil() {

    }
    public static boolean signTaskRetryWithTimestamp(String taskId, ClientMongoOperator clientMongoOperator) {
        if (null == taskId) return true;
        if (null == clientMongoOperator) return true;
        TaskDto taskDto = clientMongoOperator.findOne(signTaskRetryQuery(taskId), ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        return judgeTaskRetryStartTime(taskDto);
    }

    public static boolean judgeTaskRetryStartTime(TaskDto taskDto) {
        if (null == taskDto) return true;
        Long retryStartTimeFlag = taskDto.getTaskRetryStartTimeFlag();
        return null == retryStartTimeFlag || retryStartTimeFlag <= 0;
    }

    public static Query signTaskRetryQuery(String taskId) {
        if (null == taskId) return null;
        ObjectId objectId = new ObjectId(taskId);
        Criteria criteria = where("_id").is(objectId);
        return Query.query(criteria);
    }

    public static Update signTaskRetryUpdate(boolean withTimestamp, long retryStartTime) {
        Update update = new Update();
        update.set("taskRetryStatus", TaskDto.RETRY_STATUS_RUNNING);
        if (withTimestamp) {
            update.set("taskRetryStartTime", retryStartTime);
        }
        return update;
    }
}
