package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.tapdata.entity.*;
import com.tapdata.entity.dataflow.batch.BatchOffsetUtil;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.aspect.SourceStateAspect;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.dao.DoSnapshotFunctions;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.exception.TapConcurrentReadTableExCode_36;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HazelcastSourceConcurrentReadDataNode extends HazelcastSourcePdkDataNode{
    private static final Logger logger = LogManager.getLogger(HazelcastSourceConcurrentReadDataNode.class);
    protected int concurrentReadThreadNumber;
    protected LinkedBlockingQueue<String> tapTableQueue = new LinkedBlockingQueue<>();
    protected ExecutorService concurrentReadThreadPool;
    private DoSnapshotFunctions functions;
    public HazelcastSourceConcurrentReadDataNode(DataProcessorContext dataProcessorContext) {
        super(dataProcessorContext);
        Node<?> node = dataProcessorContext.getNode();
        if (!(node instanceof DatabaseNode)) {
            throw new TapCodeException(TapConcurrentReadTableExCode_36.ILLEGAL_NODE_TYPE, "Expected DatabaseNode, actual node type is: " + node.getClass().getName());
        }
        this.concurrentReadThreadNumber = ((DatabaseNode) node).getConcurrentReadThreadNumber();
        this.concurrentReadThreadPool = new ThreadPoolExecutor(concurrentReadThreadNumber, concurrentReadThreadNumber, 30L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    }

    @Override
    protected void doSnapshot(List<String> tableList) {
        functions = checkFunctions(tableList);
        tapTableQueue.addAll(tableList);
        startConcurrentReadConsumer();
    }

    protected void startConcurrentReadConsumer() {
        try {
            lockBySourceRunnerLock();
            AtomicBoolean firstBatch = new AtomicBoolean(true);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            AtomicInteger threadIndex = new AtomicInteger(1);
            for (int i = 0; i < concurrentReadThreadNumber; i++) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    Thread.currentThread().setName("[initial-read-thread]-" + threadIndex.get());
                    while (!tapTableQueue.isEmpty() && isRunning()) {
                        String tableName = "";
                        try {
                            tableName = tapTableQueue.poll(1L, TimeUnit.MILLISECONDS);
                            processDoSnapshot(tableName, firstBatch);
                        } catch (InterruptedException e) {
                            obsLogger.warn("Initial concurrent read thread interrupted", e);
                            Thread.currentThread().interrupt();
                        } catch (Throwable e) {
                            logger.error("Initial concurrent read failed, table : {}", tableName, e.getCause());
                            errorHandle(e.getCause());
                            break;
                        }
                    }
                }, concurrentReadThreadPool);
                futures.add(future);
                threadIndex.incrementAndGet();
            }
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allOf.join();
            if (isRunning()) {
                enqueue(new TapdataCompleteSnapshotEvent());
            }
            AspectUtils.executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_COMPLETED));
            executeAspect(new SnapshotReadEndAspect().dataProcessorContext(dataProcessorContext));
        } finally {
            endSnapshotLoop.set(true);
            unLockBySourceRunnerLock();
        }
    }

    @Override
    protected boolean isRunning() {
        return super.isRunning();
    }

    protected void processDoSnapshot(String tableName, AtomicBoolean firstBatch) throws Throwable {
        if (StringUtils.isEmpty(tableName)) return;
        TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
        TapTable tapTable = tapTableMap.get(tableName);
        String tableId = tapTable.getId();
        if (BatchOffsetUtil.batchIsOverOfTable(syncProgress, tableId)) {
            obsLogger.info("Skip table [{}] in batch read, reason: last task, this table has been completed batch read",
                    tableId);
            return;
        }
        firstBatch.set(true);
        try {
            executeAspect(new SnapshotReadTableBeginAspect().dataProcessorContext(dataProcessorContext).tableName(tableName));
            if (this.removeTables != null && this.removeTables.contains(tableName)) {
                obsLogger.info("Table {} is detected that it has been removed, the snapshot read will be skipped", tableName);
                this.removeTables.remove(tableName);
                return;
            }
            obsLogger.info("Starting batch read, table name: {}", tableId);
            doSnapshotInvoke(tableName, functions, tapTable, firstBatch, tableId);
        } catch (Throwable throwable) {
            handleThrowable(tableName, throwable);
        } finally {
            unLockBySourceRunnerLock();
        }
    }

    @Override
    public void doClose() throws TapCodeException {
        if (null != concurrentReadThreadPool) {
            concurrentReadThreadPool.shutdown();
        }
        super.doClose();
    }
}
