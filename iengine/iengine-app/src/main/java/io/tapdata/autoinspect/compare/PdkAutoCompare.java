package io.tapdata.autoinspect.compare;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IPdkConnector;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.constants.Constants;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.LinkedHashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 14:30 Create
 */
public class PdkAutoCompare implements IAutoCompare {
    private static final Logger logger = LogManager.getLogger(PdkAutoCompare.class);
    private final ClientMongoOperator clientMongoOperator;
    private final AutoInspectProgress progress;
    private final long diffMaxSize;
    private final LinkedBlockingQueue<TaskAutoInspectResultDto> compareQueue = new LinkedBlockingQueue<>(1000);
    private final Supplier<Boolean> isRunning;
    private final BiConsumer<Throwable, String> errorHandle;

    public PdkAutoCompare(@NonNull ClientMongoOperator clientMongoOperator, @NonNull AutoInspectProgress progress, @NonNull IPdkConnector sourceConnector, @NonNull IPdkConnector targetConnector, @NonNull Supplier<Boolean> isRunning, BiConsumer<Throwable, String> errorHandle) {
        this.clientMongoOperator = clientMongoOperator;
        this.progress = progress;
        this.diffMaxSize = 1000;
        this.isRunning = isRunning;
        this.errorHandle = errorHandle;

        new Thread(() -> {
            while (isRunning.get()) {
                try {
                    TaskAutoInspectResultDto dto = compareQueue.poll(1000, TimeUnit.MILLISECONDS);
                    if (null == dto) continue;
                    long delay = 5000 - (System.currentTimeMillis() - dto.getCreated().getTime());
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }

                    LinkedHashSet<String> keyNames = new LinkedHashSet<>(dto.getTargetKeymap().keySet());
                    CompareRecord sourceRecord = dto.toSourceRecord();

                    // refresh target record and compare
                    CompareRecord targetRecord = targetConnector.queryByKey(dto.getTargetTableName(), dto.getTargetKeymap(), keyNames);
                    if (null == targetRecord) {
                        // ignore if record not exists in target and source
                        if (null == sourceConnector.queryByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames)) {
                            logger.info("Fix record not exists in source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                            fix(dto);
                            continue;
                        }
                        logger.info("Not found in target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                    } else if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
                        logger.info("Fix in query target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                        fix(dto);
                        continue; // fix difference
                    } else {
                        // refresh target record to result
                        dto.fillTarget(targetRecord);

                        // refresh source record and compare
                        sourceRecord = sourceConnector.queryByKey(sourceRecord.getTableName(), sourceRecord.getOriginalKey(), keyNames);
                        if (null != sourceRecord) {
                            if (CompareStatus.Ok == sourceRecord.compare(targetRecord)) {
                                logger.info("Fix in query source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                                fix(dto);
                                continue; // fix difference
                            }

                            // refresh target record to result
                            dto.fillSource(sourceRecord);
                        }
                    }

                    // if failed, save record
                    save(dto);
                } catch (Throwable e) {
                    errorHandle.accept(e, "Exit auto compare");
                    break;
                }
            }
        }).start();
    }

    @Override
    public void autoCompare(@NonNull TaskAutoInspectResultDto dto) {
        while (true) {
            if (!isRunning.get()) {
                logger.warn("{} is not running", Constants.MODULE_NAME);
                return;
            }
            try {
                if (compareQueue.offer(dto, 5000, TimeUnit.MILLISECONDS)) {
                    break;
                }
                logger.warn("{} queue is full", Constants.MODULE_NAME);
            } catch (InterruptedException e) {
                errorHandle.accept(e, "Exit auto compare enqueue");
                return;
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    public void fix(@NonNull TaskAutoInspectResultDto dto) {
        CompareTableItem tableItem = progress.getTableItem(dto.getOriginalTableName());
        if (tableItem.getDiffKeys().contains(dto.getOriginalKeymap())) {
            Query query = Query.query(Criteria
                    .where("taskId").is(dto.getTaskId())
                    .and("originalTableName").is(dto.getOriginalTableName())
                    .and("originalKeymap").is(dto.getOriginalKeymap())
            );
            clientMongoOperator.delete(query, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
            tableItem.removeDiff(dto.getOriginalKeymap());
        }
    }

    private void save(@NonNull TaskAutoInspectResultDto dto) {
        CompareTableItem tableItem = progress.getTableItem(dto.getOriginalTableName());
        if (tableItem.getDiffCounts() >= diffMaxSize) {
            throw AutoInspectException.diffMaxSize(diffMaxSize, tableItem);
        }
        logger.info("Store AutoInspectResult '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
        //bug: upsert api can not save most properties
        clientMongoOperator.insertOne(dto, ConnectorConstant.AUTO_INSPECT_RESULTS_COLLECTION);
        tableItem.addDiff(dto.getOriginalKeymap());
    }
}
