package io.tapdata.observable.metric;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.sample.BulkReporter;
import io.tapdata.common.sample.request.BulkRequest;
import io.tapdata.entity.logger.TapLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Dexter
 */
public class TaskSampleReporter implements BulkReporter {
    private final Logger logger = LogManager.getLogger(TaskSampleReporter.class);

    private final ClientMongoOperator operator;

    public TaskSampleReporter(ClientMongoOperator operator) {
        this.operator = operator;
    }

    @Override
    public void execute(BulkRequest bulkRequest) {
        if (bulkRequest == null) {
            logger.warn("The bulk request is null, checkout if the data si not set.");
            return;
        }
        if (bulkRequest.getSamples().isEmpty() && bulkRequest.getStatistics().isEmpty()) {
            logger.info("The bulk request is empty, skip report process.");
            return;
        }
//        TapLogger.info("XXXX", "bulkRequest " + bulkRequest.toString());
        try {
            operator.insertOne(bulkRequest, ConnectorConstant.SAMPLE_STATISTIC_COLLECTION + "/points/v2");
        } catch (Exception e) {
            logger.warn("Failed to report task samples and statistics, will retry...");
        }
    }
}
