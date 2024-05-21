package io.tapdata.flow.engine.util;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.ErrorEvent;
import io.tapdata.observable.logging.ObsLogger;
import org.bson.types.ObjectId;

import java.util.List;

public class TaskDtoUtil {
    public static void updateErrorEvent(ClientMongoOperator clientMongoOperator, List<ErrorEvent> errorEvents, ObjectId taskId, ObsLogger obsLogger, String errorMsg) {
        try {
            clientMongoOperator.insertOne(errorEvents, ConnectorConstant.TASK_COLLECTION + "/errorEvents/" + taskId);
        } catch (Exception e){
            obsLogger.warn(errorMsg, e.getMessage());
        }
    }
}
