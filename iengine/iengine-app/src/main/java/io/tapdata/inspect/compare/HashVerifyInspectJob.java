package io.tapdata.inspect.compare;

import com.tapdata.entity.inspect.InspectStatus;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.error.TaskInspectExCode_27;
import io.tapdata.exception.TapCodeException;
import io.tapdata.inspect.InspectJob;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.util.InspectJobUtil;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.QueryHashByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HashVerifyInspectJob extends InspectJob {
    private static final String TAG = HashVerifyInspectJob.class.getSimpleName();
    private static final Logger logger = LogManager.getLogger(HashVerifyInspectJob.class);
    private final AtomicBoolean lock = new AtomicBoolean(true);

    public HashVerifyInspectJob(InspectTaskContext inspectTaskContext) {
        super(inspectTaskContext);
    }

    @Override
    protected void doRun() {
        try {
            final AtomicInteger retry = new AtomicInteger(0);
            while (retry.get() < 4) {
                try {
                    CompletableFuture.supplyAsync(() -> doSourceHash(retry))
                            .thenAcceptBoth(CompletableFuture.supplyAsync(() -> doTargetHash(retry)), this::doHashVerify);
                    while (lock.get()) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(10);
                        } catch (InterruptedException interruptedException) {
                        }
                    }
                    break;
                } catch (Exception e) {
                    if (doWhenException(retry, e)) {
                        break;
                    }
                }
            }
        } catch (Throwable e) {
            logger.error("Inspect failed " + name, e);
        }
    }

    protected TapHashResult doSourceHash(final AtomicInteger retry) {
        AtomicReference<TapHashResult> sourceHash = new AtomicReference<>();
        List<QueryOperator> srcConditions = inspectTask.getSource().getConditions();
        TapTable srcTable = InspectJobUtil.getTapTable(inspectTask.getSource(), inspectTaskContext);
        QueryHashByAdvanceFilterFunction queryHashOfSource = this.sourceNode.getConnectorFunctions().getQueryHashByAdvanceFilterFunction();
        if (null == queryHashOfSource) {
            retry.set(3);
            throw new TapCodeException(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION,
                    "Source node does not support hash verification with filter function: " + sourceNode.getConnectorContext().getSpecification().getId());
        }
        TapAdvanceFilter tapAdvanceFilter = InspectJobUtil.wrapFilter(srcConditions);
        PDKInvocationMonitor.invoke(this.sourceNode, PDKMethod.QUERY_HASH_BY_ADVANCE_FILTER,
                () -> queryHashOfSource.query(this.sourceNode.getConnectorContext(), tapAdvanceFilter, srcTable, sourceHash::set), TAG);
        return sourceHash.get();

    }

    protected TapHashResult doTargetHash(final AtomicInteger retry) {
        AtomicReference<TapHashResult> targetHash = new AtomicReference<>();
        List<QueryOperator> tgtConditions = inspectTask.getTarget().getConditions();
        TapTable tgtTable = InspectJobUtil.getTapTable(inspectTask.getTarget(), inspectTaskContext);
        QueryHashByAdvanceFilterFunction queryHashOfTarget = this.targetNode.getConnectorFunctions().getQueryHashByAdvanceFilterFunction();
        if (null == queryHashOfTarget) {
            retry.set(3);
            throw new TapCodeException(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION,
                    "Target node does not support hash verification with filter function: " + targetNode.getConnectorContext().getSpecification().getId());
        }
        TapAdvanceFilter filter = InspectJobUtil.wrapFilter(tgtConditions);
        PDKInvocationMonitor.invoke(this.targetNode, PDKMethod.QUERY_HASH_BY_ADVANCE_FILTER,
                () -> queryHashOfTarget.query(this.targetNode.getConnectorContext(), filter, tgtTable, targetHash::set), TAG);
        return targetHash.get();
    }

    protected void doHashVerify(TapHashResult sourceHash, TapHashResult targetHash) {
        try {
            boolean passed = false;
            if (null != sourceHash && null != targetHash && null != sourceHash.getHash()) {
                passed = sourceHash.getHash().equals(targetHash.getHash());
            }
            logger.debug("Source table hash: {}, target table hash: {}",
                    null == sourceHash ? "" : sourceHash.getHash(),
                    null == sourceHash ? "" : sourceHash.getHash());
            stats.setEnd(new Date());
            stats.setStatus("done");
            stats.setResult(passed ? "passed" : "failed");
            stats.setProgress(1);
        } finally {
            lock.set(false);
        }
    }

    protected boolean doWhenException(final AtomicInteger retry, Exception e) {
        if (retry.get() >= 3) {
            logger.error(String.format("Failed to compare the hash of rows in table %s.%s and table %s.%s, the taskId is %s",
                    source.getName(), inspectTask.getSource().getTable(),
                    target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()), e);
            stats.setEnd(new Date());
            stats.setStatus(InspectStatus.ERROR.getCode());
            stats.setResult("failed");
            stats.setErrorMsg(e.getMessage() + "\n" +
                    Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n")));
            return true;
        }
        retry.incrementAndGet();
        stats.setErrorMsg(String.format("Check has an exception and is trying again, The number of retries: %s", retry));
        stats.setStatus(InspectStatus.ERROR.getCode());
        stats.setEnd(new Date());
        stats.setResult("failed");
        progressUpdateCallback.progress(inspectTask, stats, null);
        logger.error(String.format("Check has an exception and is trying again,  The number of retries: %s", retry), e);
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException interruptedException) {
            return true;
        }
        return false;
    }
}
