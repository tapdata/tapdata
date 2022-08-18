package io.tapdata.autoinspect.compare.impl;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.compare.IAutoCompare;
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
    private final IConnector sourceConnector;
    private final IConnector targetConnector;

    public AutoPdkCompare(ClientMongoOperator clientMongoOperator, IConnector sourceConnector, IConnector targetConnector) {
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
//            Criteria criteria = Criteria.where("taskId").is(dto.getTaskId())
//                    .and("originalTableName").is(dto.getOriginalTableName());
//            for (Map.Entry<String, Object> en : dto.getOriginalKeymap().entrySet()) {
//                criteria.and("originalKeymap." + en.getKey()).is(en.getValue());
//            }
//            Query query = Query.query(criteria);
//            Map<String, Object> update = MapUtil.obj2Map(dto);
//            update.remove("id");
//            clientMongoOperator.upsert(query.getQueryObject(), update, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
            clientMongoOperator.insertOne(dto, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
        } catch (Exception e) {
            logger.warn("save compare results failed: {}", e.getMessage(), e);
        }
    }
}
