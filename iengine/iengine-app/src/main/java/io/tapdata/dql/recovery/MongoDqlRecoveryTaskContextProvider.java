package io.tapdata.dql.recovery;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.springframework.data.mongodb.core.query.Query;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/** Reads the task assignment/version through the existing Engine TM client. */
public class MongoDqlRecoveryTaskContextProvider implements DqlRecoveryTaskContextProvider {
    private final ClientMongoOperator clientMongoOperator;

    public MongoDqlRecoveryTaskContextProvider(ClientMongoOperator clientMongoOperator) {
        this.clientMongoOperator = clientMongoOperator;
    }

    @Override
    public DqlRecoveryTaskContext find(String taskId) {
        if (clientMongoOperator == null || taskId == null || taskId.isBlank()) {
            return null;
        }
        TaskDto task = clientMongoOperator.findOne(
                Query.query(where("_id").is(taskId)),
                ConnectorConstant.TASK_COLLECTION,
                TaskDto.class
        );
        return task == null ? null : new DqlRecoveryTaskContext(taskId, task.getVersion(), task.getAgentId());
    }
}
