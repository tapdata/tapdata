package io.tapdata.autoinspect.compare;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 14:30 Create
 */
public class AutoPdkCompare implements IAutoCompare {
    private static final Logger logger = LogManager.getLogger(AutoPdkCompare.class);
    private final ClientMongoOperator clientMongoOperator;
    private final IPdkConnector sourceConnector;
    private final IPdkConnector targetConnector;

    public AutoPdkCompare(ClientMongoOperator clientMongoOperator, IPdkConnector sourceConnector, IPdkConnector targetConnector) {
        this.clientMongoOperator = clientMongoOperator;
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    @Override
    public void add(@NonNull TaskAutoInspectResultDto taskAutoInspectResultDto) {
        logger.info("save dto: {}", taskAutoInspectResultDto);
        save(taskAutoInspectResultDto);
    }

    @Override
    public void close() throws Exception {

    }

    private void save(TaskAutoInspectResultDto dto) {
        try {
            //bug: upsert api can not save most properties
            clientMongoOperator.insertOne(dto, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
        } catch (Exception e) {
            logger.warn("save compare results failed: {}", e.getMessage(), e);
        }
    }
}
