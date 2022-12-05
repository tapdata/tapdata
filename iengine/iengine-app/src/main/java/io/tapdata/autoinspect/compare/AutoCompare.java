package io.tapdata.autoinspect.compare;

import com.alibaba.fastjson.JSON;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.compare.IQueryCompare;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 14:30 Create
 */
public class AutoCompare implements IAutoCompare {
    private static final Logger logger = LogManager.getLogger(AutoCompare.class);
    private final ClientMongoOperator clientMongoOperator;
    private final AutoInspectProgress progress;
    private final long diffMaxSize;
    private final LinkedBlockingQueue<TaskAutoInspectResultDto> compareQueue = new LinkedBlockingQueue<>(1000);
    private final Supplier<Boolean> supperRunning;
    private final BiConsumer<Throwable, String> errorHandle;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean forceStopping = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public AutoCompare(@NonNull ClientMongoOperator clientMongoOperator, @NonNull AutoInspectProgress progress, @NonNull IQueryCompare queryCompare, @NonNull Supplier<Boolean> supperRunning, BiConsumer<Throwable, String> errorHandle) {
        this.clientMongoOperator = clientMongoOperator;
        this.progress = progress;
        this.diffMaxSize = 1000;
        this.supperRunning = supperRunning;
        this.errorHandle = errorHandle;

        new Thread(() -> {
            try {
                while (isRunning()) {
                    try {
                        TaskAutoInspectResultDto dto = compareQueue.poll(1000, TimeUnit.MILLISECONDS);
                        if (null == dto) {
                            if (stopping.get()) break;
                            continue;
                        }
                        long delay = 5000 - (System.currentTimeMillis() - dto.getCreateAt().getTime());
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }

                        switch (queryCompare.queryCompare(dto)) {
                            case Deleted:
                                logger.debug("Fix record not exists in source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                                fix(dto);
                                break;
                            case FixTarget:
                                logger.debug("Fix in query target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                                fix(dto);
                                break;
                            case FixSource:
                                logger.debug("Fix in query source and target '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
                                fix(dto);
                                break;
                            case Diff:
                                save(dto);
                                break;
                        }
                    } catch (Throwable e) {
                        stop(true);
                        errorHandle.accept(e, "Exit auto compare, " + e.getMessage());
                        break;
                    }
                }
            } finally {
                completed.set(true);
            }
        }).start();
    }

    @Override
    public void autoCompare(@NonNull TaskAutoInspectResultDto dto) {
        while (true) {
            if (!isRunning()) {
                logger.warn("{} is not running", AutoInspectConstants.MODULE_NAME);
                return;
            }
            try {
                if (compareQueue.offer(dto, 5000, TimeUnit.MILLISECONDS)) {
                    break;
                }
                logger.warn("{} queue is full", AutoInspectConstants.MODULE_NAME);
            } catch (InterruptedException e) {
                stop(true);
                errorHandle.accept(e, "Exit auto compare enqueue");
                return;
            }
        }
    }

    private boolean isRunning() {
        return supperRunning.get() && !forceStopping.get();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public boolean stop(boolean force) {
        if (force) {
            forceStopping.compareAndSet(false, true);
        }
        stopping.compareAndSet(false, true);
        return completed.get();
    }

    public void fix(@NonNull TaskAutoInspectResultDto dto) {
        CompareTableItem tableItem = progress.getTableItem(dto.getOriginalTableName());
        if (tableItem.getDiffKeys().contains(dto.getOriginalKeymap())) {
            Query query = Query.query(Criteria
                    .where("taskId").is(dto.getTaskId())
                    .and("originalTableName").is(dto.getOriginalTableName())
                    .and("originalKeymap").is(dto.getOriginalKeymap())
            );
            clientMongoOperator.delete(query, AutoInspectConstants.AUTO_INSPECT_RESULTS_COLLECTION_NAME);
            tableItem.removeDiff(dto.getOriginalKeymap());
        }
    }

    private void save(@NonNull TaskAutoInspectResultDto dto) {
        CompareTableItem tableItem = progress.getTableItem(dto.getOriginalTableName());
        if (tableItem.getDiffCounts() >= diffMaxSize) {
            throw AutoInspectException.diffMaxSize(diffMaxSize, tableItem);
        }
        logger.debug("Store AutoInspectResult '{}': {}", dto.getOriginalTableName(), JSON.toJSONString(dto.getOriginalKeymap()));
        //bug: upsert api can not save most properties
        clientMongoOperator.insertOne(dto, AutoInspectConstants.AUTO_INSPECT_RESULTS_COLLECTION_NAME);
        tableItem.addDiff(dto.getOriginalKeymap());
    }
}
