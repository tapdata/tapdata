package io.tapdata.inspect.compare;

import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectStatus;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.error.TaskInspectExCode_27;
import io.tapdata.exception.TapCodeException;
import io.tapdata.inspect.InspectJob;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.util.InspectJobUtil;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.common.QueryHashByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HashVerifyInspectJob extends InspectJob {
    private static String TAG = HashVerifyInspectJob.class.getSimpleName();
    private Logger logger = LogManager.getLogger(HashVerifyInspectJob.class);
    protected static final String FAILED_TAG = "failed";
    protected static final String SUCCEED_TAG = "passed";

    public HashVerifyInspectJob(InspectTaskContext inspectTaskContext) {
        super(inspectTaskContext);
    }

    @Override
    protected void doRun() {
        try {
            CompletableFuture.supplyAsync(this::doSourceHash)
                    .thenAcceptBoth(CompletableFuture.supplyAsync(this::doTargetHash), this::doHashVerify).get();
        } catch (Exception e) {
            doWhenException(e);
        }
    }

    protected TapHashResult<String> doSourceHash() {
        return doGetHash(inspectTask.getSource(), this.sourceNode, "Source node does not support hash verification with filter function: ");
    }

    protected TapHashResult<String> doTargetHash() {
        return doGetHash(inspectTask.getTarget(), this.targetNode, "Target node does not support hash verification with filter function: ");
    }

    protected TapHashResult<String> doGetHash(InspectDataSource dataSource, ConnectorNode node, String errorMsg) {
        AtomicReference<TapHashResult<String>> hashResultAtomicReference = new AtomicReference<>();
        List<QueryOperator> conditions = dataSource.getConditions();
        TapTable table = InspectJobUtil.getTapTable(dataSource, inspectTaskContext);
        QueryHashByAdvanceFilterFunction hashFunction = node.getConnectorFunctions().getQueryHashByAdvanceFilterFunction();
        TapConnectorContext connectorContext = node.getConnectorContext();
        if (null == hashFunction) {
            throw new TapCodeException(TaskInspectExCode_27.CONNECTOR_NOT_SUPPORT_FUNCTION, errorMsg + connectorContext.getSpecification().getId());
        }
        TapAdvanceFilter filter = InspectJobUtil.wrapFilter(conditions);
        if (Boolean.TRUE.equals(dataSource.isEnableCustomCommand())) {
            filter.match(DataMap.create().kv("customCommand", dataSource.getCustomCommand()));
        }
        PDKInvocationMonitor.invoke(node, PDKMethod.QUERY_HASH_BY_ADVANCE_FILTER,
                () -> hashFunction.query(connectorContext, filter, table, hashResultAtomicReference::set), TAG);
        return hashResultAtomicReference.get();
    }

    protected void doHashVerify(TapHashResult<String> sourceHash, TapHashResult<String> targetHash) {
        boolean passed = false;
        if (null != sourceHash && null != targetHash && null != sourceHash.getHash()) {
            passed = sourceHash.getHash().equals(targetHash.getHash());
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Source table hash: {}, target table hash: {}",
                    null == sourceHash ? "" :String.valueOf(sourceHash.getHash()),
                    null == sourceHash ? "" : String.valueOf(sourceHash.getHash()));
        }
        stats.setEnd(new Date());
        stats.setStatus("done");
        stats.setResult(passed ? SUCCEED_TAG : FAILED_TAG);
        stats.setProgress(1);
    }

    protected void doWhenException(Exception e) {
        InspectDataSource s = inspectTask.getSource();
        InspectDataSource t = inspectTask.getTarget();
        logger.warn("Failed to compare the hash of rows in table {}.{} and table {}.{}, the taskId is {}",
                null == source ? "" : source.getName(),
                null == s ? "" : s.getTable(),
                null == target ? "" : target.getName(),
                null == t ? "" : t.getTable(),
                inspectTask.getTaskId());
        stats.setEnd(new Date());
        stats.setStatus(InspectStatus.ERROR.getCode());
        stats.setResult(FAILED_TAG);
        stats.setErrorMsg(e.getMessage() + "\n" +
                Arrays.stream(e.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n")));
    }
}
