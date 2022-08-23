package io.tapdata.autoinspect.compare;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 14:30 Create
 */
public class PdkAutoCompare implements IAutoCompare {
    private static final Logger logger = LogManager.getLogger(PdkAutoCompare.class);
    private final ClientMongoOperator clientMongoOperator;
    private final IPdkConnector sourceConnector;
    private final IPdkConnector targetConnector;

    public PdkAutoCompare(ClientMongoOperator clientMongoOperator, IPdkConnector sourceConnector, IPdkConnector targetConnector) {
        this.clientMongoOperator = clientMongoOperator;
        this.sourceConnector = sourceConnector;
        this.targetConnector = targetConnector;
    }

    @Override
    public void autoCompare(@NonNull TaskAutoInspectResultDto dto) {
        LinkedHashSet<String> keyNames = new LinkedHashSet<>(dto.getTargetKeymap().keySet());
        CompareRecord sourceRecord = dto.toSourceRecord();

        // refresh target record and compare
        CompareRecord targetRecord = targetConnector.queryByKey(dto.getTargetTableName(), dto.getTargetKeymap(), keyNames);
        if (null == targetRecord) {
            // ignore if record not exists in target and source
            if (null == sourceConnector.queryByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames)) {
                logger.info("Fix record not exists in source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                return;
            }
            logger.info("Not found in target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
        } else if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
            logger.info("Fix in query target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
            return; // fix difference
        } else {
            // refresh target record to result
            dto.fillTarget(targetRecord);

            // refresh source record and compare
            sourceRecord = sourceConnector.queryByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames);
            if (null != sourceRecord) {
                if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
                    logger.info("Fix in query source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                    return; // fix difference
                }

                // refresh target record to result
                dto.fillSource(sourceRecord);
            }
        }

        // if failed, save record
        save(dto);
    }

    @Override
    public void close() throws Exception {

    }

    private void save(TaskAutoInspectResultDto dto) {
        try {
            logger.info("Store AutoInspectResult '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
            //bug: upsert api can not save most properties
            clientMongoOperator.insertOne(dto, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
        } catch (Exception e) {
            logger.warn("Save compare results failed: {}", e.getMessage(), e);
        }
    }
}
