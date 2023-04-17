package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk;

import com.hazelcast.jet.core.Outbox;
import com.tapdata.constant.BeanUtil;
import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.DataProcessorContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.vo.ReadPartitionOptions;
import io.tapdata.aspect.*;
import io.tapdata.aspect.taskmilestones.*;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.async.master.*;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.exception.node.NodeException;
import io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition.*;
import io.tapdata.flow.engine.V2.progress.SnapshotProgressManager;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.task.TerminalMode;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionsFunction;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.entity.params.PDKMethodInvoker;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.utils.LoggerUtils;
import io.tapdata.schema.TapTableMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.insertRecordEvent;
import static io.tapdata.entity.simplify.TapSimplify.sleep;

/**
 * @author Aplomb
 *
 **/
public class HazelcastSourcePartitionReadDataNode extends HazelcastSourcePdkDataNode {
	private static final String TAG = HazelcastSourcePartitionReadDataNode.class.getSimpleName();
	public final Logger logger = LogManager.getLogger(HazelcastSourcePartitionReadDataNode.class);
	public final Object streamReadLock = new int[0];
	private static final int ASYNCLY_COUNT_SNAPSHOT_ROW_SIZE_TABLE_THRESHOLD = 100;

	private ShareCdcReader shareCdcReader;

	private final SourceStateAspect sourceStateAspect;
	private final JobMaster asyncMaster;
	private QueueWorker initialSyncWorker;
	private ParallelWorker tableParallelWorker;
	private QueueWorker streamReadWorker;

    private final Map<String, TapEventPartitionDispatcher> tableEventPartitionDispatcher = new ConcurrentHashMap<>();

	private final AtomicBoolean streamReadStarted = new AtomicBoolean(false);
	private final AtomicBoolean tableParallelWorkerStarted = new AtomicBoolean(false);
	public Integer batchSize = 5000;
	public Integer partitionReaderThreadCount = 8;
	private Map<String, ParallelWorker> tablePartitionReaderMap = new ConcurrentSkipListMap<>();
	public HazelcastSourcePartitionReadDataNode(DataProcessorContext dataProcessorContext) {
		super(dataProcessorContext);
		sourceStateAspect = new SourceStateAspect().dataProcessorContext(dataProcessorContext);
		asyncMaster = InstanceFactory.instance(JobMaster.class);
	}

	@Override
	public void startSourceRunner() {
		try {
			startAsyncJobs();
		} catch (Throwable throwable) {
			errorHandle(throwable, throwable.getMessage());
		}
	}

	@Override
	protected void doInit(@NotNull Context context) throws Exception {
		try {
			FileUtils.deleteQuietly(new File("./partition_storage/" + getNode().getId()));
			Node<?> node = dataProcessorContext.getNode();
			if(node instanceof DataParentNode) {
				DataParentNode<?> dataParentNode = (DataParentNode<?>) node;
				ReadPartitionOptions readPartitionOptions = dataParentNode.getReadPartitionOptions();
				if(readPartitionOptions != null) {
					partitionReaderThreadCount = readPartitionOptions.getPartitionThreadCount();
					batchSize = readPartitionOptions.getPartitionBatchCount();
				}
			}
			super.doInit(context);
            this.eventQueue = new LinkedBlockingQueue<>(sourceQueueCapacity >> 1);
            //this.eventQueue0 = new LinkedBlockingQueue<>(sourceQueueCapacity);
        } catch (Throwable e) {
			//Notify error for task.
			throw errorHandle(e, "init failed");
		}
	}

	private JobContext handleBatchReadForTables(JobContext jobContext) {
		ReadPartitionOptions readPartitionOptions = null;
		Node<?> node = dataProcessorContext.getNode();
		if(node instanceof DataParentNode) {
			DataParentNode<?> dataParentNode = (DataParentNode<?>) node;
			readPartitionOptions = dataParentNode.getReadPartitionOptions();
		}
		if(readPartitionOptions == null)
			readPartitionOptions = new ReadPartitionOptions();

		syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
		snapshotProgressManager = new SnapshotProgressManager(dataProcessorContext.getTaskDto(), clientMongoOperator,
				getConnectorNode(), dataProcessorContext.getTapTableMap());


		PDKSourceContext pdkSourceContext = jobContext.getContext(PDKSourceContext.class);
        List<String> pendingInitialSyncTables = pdkSourceContext.getPendingInitialSyncTables();
        obsLogger.info("Start initial sync for tables {} with readPartitionOptions {}", pendingInitialSyncTables, readPartitionOptions);
		executeAspect(new SnapshotReadBeginAspect().dataProcessorContext(dataProcessorContext).tables(pendingInitialSyncTables));

        if (null == getConnectorNode()) {
            String error = "Connector node is null";
            errorHandle(new RuntimeException(error), error);
            return jobContext;
        }

		if(getConnectorNode().getConnectorFunctions().getCountByPartitionFilterFunction() == null) {
			readPartitionOptions.setSplitType(ReadPartitionOptions.SPLIT_TYPE_BY_MINMAX);
		}
		if(readPartitionOptions.getSplitType() == ReadPartitionOptions.SPLIT_TYPE_BY_COUNT)
			doCount(pendingInitialSyncTables);

		GetReadPartitionsFunction getReadPartitionsFunction = getConnectorNode().getConnectorFunctions().getGetReadPartitionsFunction();
		if (getReadPartitionsFunction != null) {
			if (sourceRunnerFirstTime.get()) {
				executeAspect(sourceStateAspect.state(SourceStateAspect.STATE_INITIAL_SYNC_START));
			}
			tableParallelWorker = asyncMaster.createAsyncParallelWorker("Tables_" + node.getId(), partitionReaderThreadCount);

			ReadPartitionOptions finalReadPartitionOptions = readPartitionOptions;
			jobContext.foreach(pendingInitialSyncTables, tableName -> {
				if(finalReadPartitionOptions.getSplitType() == ReadPartitionOptions.SPLIT_TYPE_BY_COUNT) {
					//Wait table count finish to continue
					jobContext.foreach(Integer.MAX_VALUE, integer -> {
						sleep(100);
						return null == snapshotRowSizeMap || !snapshotRowSizeMap.containsKey(tableName);
					});
				}
				if (this.removeTables != null && this.removeTables.contains(tableName)) {
					obsLogger.info("Table " + tableName + " is detected that it has been removed, the snapshot read will be skipped");
					this.removeTables.remove(tableName);
					return null;
				}
				TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(tableName);
				if(eventPartitionDispatcher != null && eventPartitionDispatcher.isReadPartitionFinished()) {
					obsLogger.info("Table {} has read finished, no need batch read any more. ", tableName);
					return null;
				}
				Object tableOffset = ((Map<?, ?>) syncProgress.getBatchOffsetObj()).get(tableName);
				obsLogger.info("Starting batch read, table name: " + tableName + ", offset: " + tableOffset);
				tableParallelWorker.job(
                        "table#" + tableName,
                        JobContext.create(null),
                        asyncQueueWorker -> asyncQueueWorker.asyncJob((jobContext1, jobCompleted) -> {
					        handleReadPartitionsForTable(pdkSourceContext, getReadPartitionsFunction, finalReadPartitionOptions, tableName, jobCompleted);
				        })
                        .finished()
                        .setAsyncJobErrorListener((id, asyncJob, throwable) -> {
					        this.errorHandle(throwable, throwable.getMessage());
				        }
                ));
				return null;
			});
			//Go to CDC stage after all tables finished initial sync.
			tableParallelWorker.finished(this::enterCDCStageFinally);
			if(streamReadStarted.get() && !tableParallelWorkerStarted.get()) {
				synchronized (streamReadStarted) {
					if(streamReadStarted.get() && tableParallelWorkerStarted.compareAndSet(false, true))
						tableParallelWorker.start();
				}
			}
		} else {
			doSnapshot(pendingInitialSyncTables);
		}
		executeAspect(new SnapshotReadEndAspect().dataProcessorContext(dataProcessorContext));
		return null;
	}

	protected void enterCDCStageFinally() {
        // 如果有新增表， 重启connector
        if (null != newTables && !newTables.isEmpty()){
            super.handleNewTables(newTables);
        } else {
			super.enterCDCStage();
        }
	}

    protected void enterCDCStage() {

    }

	private void handleReadPartitionsForTable(PDKSourceContext pdkSourceContext, GetReadPartitionsFunction getReadPartitionsFunction, ReadPartitionOptions finalReadPartitionOptions,final String tableId, AsyncJobCompleted jobCompleted) {
		TapTable tapTable = dataProcessorContext.getTapTableMap().get(tableId);
		tablePartitionReaderMap.computeIfAbsent(tableId, table -> asyncMaster.createAsyncParallelWorker("PartitionsReader_" + table, partitionReaderThreadCount));
		ParallelWorker partitionsReader = tablePartitionReaderMap.get(tableId);//asyncMaster.createAsyncParallelWorker("PartitionsReader_" + tapTable.getId(), 8);
		obsLogger.info("Stream read started already, now start read partitions for table {}", tableId);
		AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
		tableEventPartitionDispatcher.putIfAbsent(tableId, new TapEventPartitionDispatcher(tapTable, obsLogger));
        ConnectorNode node = getConnectorNode();
        TapConnectorContext context = node.getConnectorContext();
        executeDataFuncAspect(
				GetReadPartitionsFuncAspect.class, () -> new GetReadPartitionsFuncAspect()
						.connectorContext(context)
						.dataProcessorContext(this.getDataProcessorContext())
						.splitType(finalReadPartitionOptions.getSplitType())
						.maxRecordInPartition(finalReadPartitionOptions.getMaxRecordInPartition())
						.start()
						.table(tapTable),
				getReadPartitionsFuncAspect -> PDKInvocationMonitor.invoke(
                        node,
                        PDKMethod.SOURCE_GET_READ_PARTITIONS,
						createPdkMethodInvoker().runnable(() ->
						{
							BatchReadFuncAspect batchReadFuncAspect = new BatchReadFuncAspect()
									.eventBatchSize(batchSize)
									.connectorContext(context)
									.offsetState(null)
									.dataProcessorContext(dataProcessorContext)
									.start()
									.table(tapTable);
							aspectManager.executeAspect(batchReadFuncAspect);
							List<ReadPartition> readPartitionList = new ArrayList<>();
							Runnable completedRunnable = new PartitionsCompletedRunnable(tapTable, partitionsReader, aspectManager, batchReadFuncAspect, readPartitionList, this, jobCompleted);
							Consumer<ReadPartition> partitionConsumer = new PartitionConsumer(pdkSourceContext, tapTable, partitionsReader, getReadPartitionsFuncAspect, batchReadFuncAspect, readPartitionList, tableEventPartitionDispatcher, this);
							//Recover from TM
							List<ReadPartition> recoveredPartitions = null;
							Map<String, Long> completedPartitionIds = null;
							Boolean tableCompleted = null;
							Object batchOffsetObj = syncProgress.getBatchOffsetObj();
							if(batchOffsetObj instanceof Map) {
								PartitionTableOffset partitionTableOffset = (PartitionTableOffset) ((Map<?, ?>) batchOffsetObj).get(tableId);
								if(partitionTableOffset != null) {
									recoveredPartitions = partitionTableOffset.getPartitions();
									completedPartitionIds = partitionTableOffset.getCompletedPartitions();
									tableCompleted = partitionTableOffset.getTableCompleted();
								}
								obsLogger.info("PartitionTableOffset recoveredPartitions {}, completedPartitions {}, tableCompleted {}", (recoveredPartitions != null ? recoveredPartitions.size() : 0), (completedPartitionIds != null ? completedPartitionIds.size() : 0), tableCompleted);
							}

							if(tableCompleted != null && tableCompleted) {
								//Table has been read completed, ignore this table.
							} else if(recoveredPartitions != null && !recoveredPartitions.isEmpty()) {
								//Read partition has been split, recover reading partitions.
                                Map<String, Long> completedPartitionIdsCache = Optional.ofNullable(completedPartitionIds).orElse(new HashMap<>());
								for(ReadPartition readPartition : recoveredPartitions) {
                                    String partitionId = readPartition.getId();
                                    if(completedPartitionIdsCache.containsKey(partitionId)) {
										obsLogger.info("Read partition {} has read completed, count {}", readPartition, completedPartitionIds.get(partitionId));
										continue;
									}
									partitionConsumer.accept(readPartition);
								}
								partitionsReader.start();
								completedRunnable.run();
							} else {
								//Can not recover from TM, split read partitions through PDK function.
								TypeSplitterMap typeSplitterMap = new TypeSplitterMap();
								getReadPartitionsFunction.getReadPartitions(
                                        context,
										tapTable,
										GetReadPartitionOptions.create().maxRecordInPartition(finalReadPartitionOptions.getMaxRecordInPartition())
												.minMaxSplitPieces(finalReadPartitionOptions.getMinMaxSplitPieces())
												.splitType(finalReadPartitionOptions.getSplitType())
												.typeSplitterMap(typeSplitterMap)
												.consumer(partitionConsumer)
												.completedRunnable(completedRunnable));
								partitionsReader.start();
							}
						})
				));
	}

	public void handleEnterCDCStage(ParallelWorker partitionsReader, TapTable tapTable) {
        final String tapTableId = tapTable.getId();
        obsLogger.info("All partitions has been read for table {}, stream records can pass directly to next node, without through its partition.", tapTableId);
		partitionsReader.stop();
		ParallelWorker parallelWorker = tablePartitionReaderMap.remove(tapTableId);
		if(parallelWorker != null)
			parallelWorker.stop();
		TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(tapTableId);
		if(eventPartitionDispatcher != null) {
			eventPartitionDispatcher.readPartitionFinished();
		}
	}

	private JobContext handleStreamRead(JobContext jobContext) {
		doCdc();
		return null;
	}

	private void handleWorkerError(String id, JobBase asyncJob, Throwable throwable) {
		Throwable throwableWrapper = throwable;
		if (!(throwableWrapper instanceof NodeException)) {
			throwableWrapper = new NodeException(throwableWrapper).context(getProcessorBaseContext());
		}
		//noinspection ThrowableNotThrown
		errorHandle(throwableWrapper, throwable.getMessage());
	}

	public void startAsyncJobs() {
        newTables = new CopyOnWriteArrayList<>();
        tableParallelWorkerStarted.set(false);
        if(tableParallelWorker != null) {
            tableParallelWorker.stop();
            tableParallelWorker = null;
        }
        if(streamReadWorker != null) {
            streamReadWorker.stop();
            streamReadWorker = null;
        }
        if(initialSyncWorker != null) {
            initialSyncWorker.stop();
            initialSyncWorker = null;
        }
		TapTableMap<String, TapTable> tapTableMap = dataProcessorContext.getTapTableMap();
		PDKSourceContext sourceContext = PDKSourceContext.create()
				.sourcePdkDataNode(this)
				.pendingInitialSyncTables(need2InitialSync(syncProgress) ? new ArrayList<>(tapTableMap.keySet()) : null)
				.needCDC(need2CDC());
		if(sourceContext.isNeedInitialSync()) {
			//Recover read partitions from TM
			List<ReadPartition>	recoveredPartitions = null;
			Map<String, Long> completedPartitionIds = null;
			Object batchOffsetObj = syncProgress.getBatchOffsetObj();
			if(batchOffsetObj instanceof Map) {
				Map<?, ?> batchOffset = (Map<?, ?>) batchOffsetObj;
				for(Map.Entry<?, ?> entry : batchOffset.entrySet()) {
                    Object entryValue = entry.getValue();
                    String entryKey = (String) entry.getKey();
                    if(entryValue instanceof PartitionTableOffset) {
						PartitionTableOffset partitionTableOffset = (PartitionTableOffset) entryValue;
						recoveredPartitions = partitionTableOffset.getPartitions();
						completedPartitionIds = partitionTableOffset.getCompletedPartitions();
						Boolean tableCompleted = partitionTableOffset.getTableCompleted();

						if(tableCompleted != null && tableCompleted) {
							TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(entryKey);
							if(eventPartitionDispatcher == null) {
								tableEventPartitionDispatcher.computeIfAbsent(entryKey, table -> {
									TapTable tapTable = tapTableMap.get(table);
									if(tapTable == null)
										throw new CoreException(PartitionErrorCodes.TAP_TABLE_NOT_FOUND, "TapTable {} not found while recovering partitions", table);
									TapEventPartitionDispatcher dispatcher = new TapEventPartitionDispatcher(tapTable, obsLogger);
									dispatcher.readPartitionFinished();
									obsLogger.info("Table {} read partition finished", table);
									return dispatcher;
								});
							}
						} else if(recoveredPartitions != null) {
							TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(entryKey);
							TapTable tapTable = tapTableMap.get(entryKey);
							if(tapTable == null)
								throw new CoreException(PartitionErrorCodes.TAP_TABLE_NOT_FOUND, "TapTable {} not found while recovering partitions", entryKey);
							if(eventPartitionDispatcher == null) {
								eventPartitionDispatcher = tableEventPartitionDispatcher.computeIfAbsent(entryKey, table -> {
									TapEventPartitionDispatcher dispatcher = new TapEventPartitionDispatcher(tapTable, obsLogger);
//										ReadPartitionHandler readPartitionHandler = new ReadPartitionHandler(sourceContext, tapTable, readPartition, this);
//										readPartitionHandler.finish();
//										obsLogger.info("Table {} partition {} has finished", table, readPartition);
//										dispatcher.register(readPartition, readPartitionHandler);
									return dispatcher;
								});
							}
							TapIndexEx partitionIndex = tapTable.partitionIndex();
                            Map<String, Long> completedPartitionIdsCache = completedPartitionIds != null ? completedPartitionIds : new HashMap<>();
                            for(ReadPartition readPartition : recoveredPartitions) {
								readPartition.partitionIndex(partitionIndex);
								if(completedPartitionIdsCache.containsKey(readPartition.getId())) {
									ReadPartitionHandler readPartitionHandler = ReadPartitionHandler.createReadPartitionHandler(sourceContext, tapTable, readPartition, this);
									readPartitionHandler.finish();
									obsLogger.info("Table {} partition {} has read finished by last time, count {}, will be skipped.", entryKey, readPartition, completedPartitionIdsCache.get(readPartition.getId()));
									eventPartitionDispatcher.register(readPartition, readPartitionHandler);
								} else {
									ReadPartitionHandler readPartitionHandler = ReadPartitionHandler.createReadPartitionHandler(sourceContext, tapTable, readPartition, this);
									obsLogger.info("Table {} partition {} hasn't been read by last time, will continue read. ", entryKey, readPartition);
									eventPartitionDispatcher.register(readPartition, readPartitionHandler);
								}
							}
						}
					}
				}
			}

			initialSyncWorker = asyncMaster.createAsyncQueueWorker("InitialSync " + getNode().getId()).setAsyncJobErrorListener(this::handleWorkerError)
					.job("batchRead", this::handleBatchReadForTables).finished().start(JobContext.create(null).context(sourceContext));
		}

		Snapshot2CDCAspect.execute(dataProcessorContext);

		if(sourceContext.isNeedCDC()) {
			executeAspect(new CDCReadBeginAspect().dataProcessorContext(dataProcessorContext));
			streamReadWorker = asyncMaster.createAsyncQueueWorker("StreamRead_tableSize_" + sourceContext.getPendingInitialSyncTables().size())
					.setAsyncJobErrorListener(this::handleWorkerError)
					.job(this::handleStreamRead).finished()
					.start(JobContext.create().context(
							StreamReadContext.create().streamStage(false).tables(sourceContext.getPendingInitialSyncTables())), true);
			executeAspect(new CDCReadEndAspect().dataProcessorContext(dataProcessorContext));
		} else {
			BeanUtil.getBean(TapdataTaskScheduler.class).getTaskClient(dataProcessorContext.getTaskDto().getId().toHexString()).terminalMode(TerminalMode.COMPLETE);
		}
	}

	@Override
	protected StreamReadConsumer generateStreamReadConsumer(ConnectorNode connectorNode, PDKMethodInvoker pdkMethodInvoker) {
		return StreamReadConsumer.create(this::handleStreamEventsReceivedDuringPartition).stateListener((oldState, newState) -> {
			if (StreamReadConsumer.STATE_STREAM_READ_ENDED != newState) {
				PDKInvocationMonitor.invokerRetrySetter(pdkMethodInvoker);
			}
			if (null != newState && StreamReadConsumer.STATE_STREAM_READ_STARTED == newState) {
				// MILESTONE-READ_CDC_EVENT-FINISH
				if (streamReadFuncAspect != null)
					executeAspect(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAM_STARTED).streamStartedTime(System.currentTimeMillis()));
				TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
//												MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
				logger.info("Connector start stream read succeed: {}", connectorNode);
				obsLogger.info("Connector {} incremental start succeed, tables: {}, data change syncing", connectorNode.getTapNodeInfo().getTapNodeSpecification().getName(), streamReadFuncAspect != null ? streamReadFuncAspect.getTables() : null);

				//start pending partition reader workers as stream read is started.
				streamReadStarted.compareAndSet(false, true);
				if (tableParallelWorker != null && !tableParallelWorkerStarted.get()) {
					synchronized (streamReadStarted) {
						if (tableParallelWorkerStarted.compareAndSet(false, true)) {
							tableParallelWorker.start();
						}
					}
				}
			}
		});
	}

	private void handleStreamEventsReceivedDuringPartition(List<TapEvent> events, Object offset) {
		if(syncProgress.getSyncStage().equals(SyncStage.CDC.name())) {
			handleStreamEventsReceived(events, offset);
			return;
		}
		Map<String, List<TapEvent>> tableEvents = new LinkedHashMap<>();
		List<TapEvent> otherEvents = new ArrayList<>();
		for(TapEvent event : events) {
			if(event instanceof TapBaseEvent) {
				String table = ((TapBaseEvent) event).getTableId();
				TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(table);
				if(eventPartitionDispatcher != null) {
					List<TapEvent> eventList = tableEvents.get(table);
					if(eventList == null)
						eventList = tableEvents.computeIfAbsent(table, k -> new ArrayList<>());
					eventList.add(event);
				}
			} else {
				otherEvents.add(event);
			}
		}
		for(Map.Entry<String, List<TapEvent>> entry : tableEvents.entrySet()) {
			TapEventPartitionDispatcher eventPartitionDispatcher = tableEventPartitionDispatcher.get(entry.getKey());
			if(eventPartitionDispatcher.isReadPartitionFinished()) {
				handleStreamEventsReceived(entry.getValue(), offset);
			} else {
				eventPartitionDispatcher.receivedTapEvents(entry.getValue());
			}
		}
		if(!otherEvents.isEmpty())
			handleStreamEventsReceived(otherEvents, offset);
		//save offset
		syncProgress.setStreamOffsetObj(offset);
	}

//    public long handleStreamInsertEventsReceived(List<Map<String, Object>> events, Object offsetObj, String tableId){
//        long cast = 0;
//        try {
//            if (events != null && !events.isEmpty()) {
//                List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.CDC, offsetObj, tableId);
//                if (logger.isDebugEnabled()) {
//                    logger.debug("Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
//                }
//                if (streamReadFuncAspect != null)
//                    AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);
//                if (CollectionUtils.isNotEmpty(tapdataEvents)) {
//                    long s = System.currentTimeMillis();
//                    for (TapdataEvent tapdataEvent : tapdataEvents) {
//                        this.enqueue(tapdataEvent);
//                    }
//                    cast = System.currentTimeMillis() - s;
//                    if (streamReadFuncAspect != null)
//                        AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_ENQUEUED).getStreamingEnqueuedConsumers(), tapdataEvents);
//                }
//            }
//        } catch (Throwable throwable) {
//            errorHandle(throwable, "Error processing incremental data, error: " + throwable.getMessage());
//        }
//        return cast;
//    }

	public void handleStreamEventsReceived(List<TapEvent> events, Object offsetObj) {
		try {
//			while (isRunning()) {
//				try {
//					if (sourceRunnerLock.tryLock(1L, TimeUnit.SECONDS)) {
//						break;
//					}
//				} catch (InterruptedException e) {
//					break;
//				}
//			}
			if (events != null && !events.isEmpty()) {
				events.forEach(event -> {
					if (null == event.getTime()) {
						throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(event);
					}
					event.addInfo("eventId", UUID.randomUUID().toString());
				});

				if (streamReadFuncAspect != null) {
					AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), events);
				}

				List<TapdataEvent> tapdataEvents = wrapTapdataEvent(events, SyncStage.CDC, offsetObj);
				if (logger.isDebugEnabled()) {
					logger.debug("Stream read {} of events, {}", events.size(), LoggerUtils.sourceNodeMessage(getConnectorNode()));
				}

				if (streamReadFuncAspect != null)
					AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_PROCESS_COMPLETED).getStreamingProcessCompleteConsumers(), tapdataEvents);

				if (CollectionUtils.isNotEmpty(tapdataEvents)) {
					tapdataEvents.forEach(this::enqueue);
//					syncProgress.setStreamOffsetObj(offsetObj);
					if (streamReadFuncAspect != null)
						AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_ENQUEUED).getStreamingEnqueuedConsumers(), tapdataEvents);
				}
			}
		} catch (Throwable throwable) {
			errorHandle(throwable, "Error processing incremental data, error: " + throwable.getMessage());
		} /*finally {
			try {
				sourceRunnerLock.unlock();
			} catch (Exception ignored) {
			}
		}*/
	}

    private List<TapdataEvent> wrapTapdataEvent(List<Map<String, Object>> events, SyncStage syncStage, Object offsetObj, String tableId) {
        int size = events.size();
        String nodeId = null;
        if (processorBaseContext.getNode() != null) {
            nodeId = processorBaseContext.getNode().getId();
        }
        List<TapdataEvent> tapdataEvents = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Map<String, Object> tapEventMap = events.get(i);
            TapInsertRecordEvent tapEvent = insertRecordEvent(tapEventMap, tableId).referenceTime(System.currentTimeMillis());
            tapEvent.addInfo("eventId", UUID.randomUUID().toString());
            TapEvent tapEventCache = cdcDelayCalculation.filterAndCalcDelay(tapEvent, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
            boolean isLast = i == (size - 1);
            TapdataEvent tapdataEvent;
            tapdataEvent = wrapTapdataEvent(tapEventCache, syncStage, offsetObj, isLast);
            if (null == tapdataEvent) {
                continue;
            }
			tapdataEvents.add(tapdataEvent);
        }
        if (streamReadFuncAspect != null) {
            AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), tapdataEvents.stream().map(TapdataEvent::getTapEvent).collect(Collectors.toList()));
        }
        return tapdataEvents;
    }

    @NotNull
    protected void wrapTapdataEvent(List<TapEvent> events, List<TapdataEvent> tapdataEvents, SyncStage syncStage, Object offsetObj) {
        int size = events.size();
        String nodeId = null;
        if (processorBaseContext.getNode() != null) {
            nodeId = processorBaseContext.getNode().getId();
        }
        List<TapEvent> eventCache = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            TapEvent tapEvent = events.get(i);
            if (null == tapEvent.getTime()) {
                throw new NodeException("Invalid TapEvent, `TapEvent.time` should be NonNUll").context(getProcessorBaseContext()).event(tapEvent);
            }
            tapEvent.addInfo("eventId", UUID.randomUUID().toString());
            TapEvent tapEventCache = cdcDelayCalculation.filterAndCalcDelay(tapEvent, times -> AspectUtils.executeAspect(SourceCDCDelayAspect.class, () -> new SourceCDCDelayAspect().delay(times).dataProcessorContext(dataProcessorContext)));
            eventCache.add(tapEventCache);
            boolean isLast = i == (size - 1);
            TapdataEvent tapdataEvent;
            tapdataEvent = wrapTapdataEvent(tapEventCache, syncStage, offsetObj, isLast);
            if (null == tapdataEvent) {
                continue;
            }
            if (tapEvent instanceof TapRecordEvent) {
                String tableId = ((TapRecordEvent)tapEvent).getTableId();
                if(removeTables == null || !removeTables.contains(tableId)){
                    if (nodeId != null) {
                        tapdataEvent.addNodeId(nodeId);
                    }
                    tapdataEvents.add(tapdataEvent);
                }
            }
        }
        if (streamReadFuncAspect != null) {
            AspectUtils.accept(streamReadFuncAspect.state(StreamReadFuncAspect.STATE_STREAMING_READ_COMPLETED).getStreamingReadCompleteConsumers(), eventCache);
        }
    }

//    public void enqueue(TapdataEvent tapdataEvent) {
//        try {
//            while (isRunning()) {
//                if (eventQueue.offer(tapdataEvent, 3, TimeUnit.SECONDS)) {
//                    break;
//                }
//            }
//        }
//        catch (Throwable throwable) {
//            throw new NodeException(throwable).context(getDataProcessorContext()).event(tapdataEvent.getTapEvent());
//        }
//    }

//    protected boolean offer(TapdataEvent dataEvent) {
//        if (dataEvent != null) {
//            Outbox outbox = getOutbox();
//            if (null != outbox) {
//                final int bucketCount = outbox.bucketCount();
//                if (bucketCount > 1) {
//                    for (bucketIndex = Math.min(bucketIndex, bucketCount); bucketIndex < bucketCount; bucketIndex++) {
//                        final TapdataEvent cloneEvent = (TapdataEvent) dataEvent.clone();
//                        if (!tryEmit(bucketIndex, cloneEvent)) {
//                            return false;
//                        }
//                    }
//                } else if (!tryEmit(dataEvent)) {
//                    return false;
//                }
//            }
//        }
//        bucketIndex = 0; // reset to 0 of return true
//        return true;
//    }

	@Override
	public void doClose() throws Exception {
		try {
			FileUtils.deleteQuietly(new File("./partition_storage/" + getNode().getId()));

			obsLogger.info("task {} closed", dataProcessorContext.getTaskDto().getId().toHexString());
			for(ParallelWorker tablePartitionReader : tablePartitionReaderMap.values()) {
				tablePartitionReader.stop();
			}
			tablePartitionReaderMap.clear();

			if (initialSyncWorker != null) {
				initialSyncWorker.stop();
			}
			if (tableParallelWorker != null) {
				tableParallelWorker.stop();
			}
			if (streamReadWorker != null) {
				streamReadWorker.stop();
			}
		} finally {
			super.doClose();
		}
	}

	//全量分片...
	@Override
	protected boolean handleNewTables(List<String> addList){
        if (endSnapshotLoop.get()){
			syncProgress.setSyncStage(SyncStage.INITIAL_SYNC.name());
            return super.handleNewTables(addList);
        }
		for(String str : addList) {
			if(!newTables.contains(str))
				newTables.add(str);
		}
        return false;
	}

	public List<String> removeTables(){
		return removeTables;
	}


//	@SneakyThrows
//	private void doCdc() {
//		if (!isRunning()) {
//			return;
//		}
//		this.endSnapshotLoop.set(true);
//		if (null == syncProgress.getStreamOffsetObj()) {
//			throw new NodeException("Starting stream read failed, errors: start point offset is null").context(getProcessorBaseContext());
//		} else {
//			TapdataStartCdcEvent tapdataStartCdcEvent = new TapdataStartCdcEvent();
//			tapdataStartCdcEvent.setSyncStage(SyncStage.CDC);
//			enqueue(tapdataStartCdcEvent);
//		}
//		// MILESTONE-READ_CDC_EVENT-RUNNING
//		TaskMilestoneFuncAspect.execute(dataProcessorContext, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
//		MilestoneUtil.updateMilestone(milestoneService, MilestoneStage.READ_CDC_EVENT, MilestoneStatus.RUNNING);
//		syncProgress.setSyncStage(SyncStage.CDC.name());
//		Node<?> node = dataProcessorContext.getNode();
//		if (node.isLogCollectorNode()) {
//			// Mining tasks force traditional increments
//			doNormalCDC();
//		} else {
//			try {
//				// Try to start with share cdc
//				doShareCdc();
//			} catch (ShareCdcUnsupportedException e) {
//				if (e.isContinueWithNormalCdc()) {
//					// If share cdc is unavailable, and continue with normal cdc is true
//					logger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
//					obsLogger.info("Share cdc unusable, will use normal cdc mode, reason: " + e.getMessage());
//					doNormalCDC();
//				} else {
//					throw new NodeException("Read share cdc log failed: " + e.getMessage(), e).context(getProcessorBaseContext());
//				}
//			} catch (Exception e) {
//				throw new NodeException("Read share cdc log failed: " + e.getMessage(), e).context(getProcessorBaseContext());
//			}
//		}
//	}

//    public boolean complete() {
//        try {
//            TaskDto taskDto = dataProcessorContext.getTaskDto();
//            if (firstComplete) {
//                Thread.currentThread().setName(String.format("Source-Complete-%s[%s]", getNode().getName(), getNode().getId()));
//                firstComplete = false;
//            }
//            List<TapdataEvent> dataEvent = null;
//            if (!isRunning()) {
//                return null == error;
//            }
//            if (pendingEvent0 != null && !pendingEvent0.isEmpty()) {
//                dataEvent = new ArrayList<>();
//                dataEvent.addAll(pendingEvent0);
//                pendingEvent0 = null;
//            }
////            else if(pendingEvent != null){
////                dataEvent = new ArrayList<>();
////                dataEvent.add(pendingEvent);
////                pendingEvent = null;
////            }
//            else {
//                try {
//                    dataEvent = queue().poll(500, TimeUnit.MILLISECONDS);
//                    if (null == dataEvent) dataEvent = new ArrayList<>();
//                    Optional.ofNullable(eventQueue.poll(500, TimeUnit.MILLISECONDS)).ifPresent(dataEvent::add);
//                } catch (InterruptedException ignored) {
//                }
//                if (null != dataEvent && !dataEvent.isEmpty()) {
//                    // covert to tap value before enqueue the event. when the event is enqueued into the eventQueue,
//                    // the event is considered been output to the next node.
//                    TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
//                    for (TapdataEvent event : dataEvent) {
//                        TapEvent tapEvent = event.getTapEvent();
//                        tapRecordToTapValue(tapEvent, codecsFilterManager);
//                    }
//                }
//            }
//
//            if (null != dataEvent && !dataEvent.isEmpty()) {
//                for (TapdataEvent tapdataEvent : dataEvent) {
//                    if (!offer(tapdataEvent)) {
//                        if (null == pendingEvent0) pendingEvent0 = new ArrayList<>();
//                        pendingEvent0.add(tapdataEvent);
//                        continue;
//                    }
//                }
//            }
//
//            if (sourceRunnerFuture != null && sourceRunnerFuture.isDone() && sourceRunnerFirstTime.get()
//                    && (null == pendingEvent0 || pendingEvent0.isEmpty()) && eventQueue0.isEmpty()) {
//                if (TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType())) {
//                    Object residueSnapshot = getGlobalMap(getCompletedInitialKey());
//                    if (residueSnapshot instanceof Integer) {
//                        int residueSnapshotInt = (int) residueSnapshot;
//                        if (residueSnapshotInt <= 0) {
//                            this.running.set(false);
//                        }
//                    }
//                } else {
//                    this.running.set(false);
//                }
//            }
//			/*if (1 == 1) {
//				Thread.sleep(5000L);
//				throw new RuntimeException("test");
//			}*/
//        } catch (Exception e) {
//            String errorMsg = String.format("Source sync failed: %s", e.getMessage());
//            obsLogger.error(errorMsg, e);
////			throw new RuntimeException(errorMsg, e);
//            errorHandle(e, errorMsg);
//        } finally {
//            ThreadContext.clearAll();
//        }
//
//        return false;
//    }
//    public void startSourceConsumer() {
//        while (isRunning()) {
//            try {
//                List<TapdataEvent> dataEvent;
//                AtomicBoolean isPending = new AtomicBoolean();
//                if (pendingEvent0 != null && !pendingEvent0.isEmpty()) {
//                    dataEvent = new ArrayList<>();
//                    dataEvent.addAll(pendingEvent0);
//                    pendingEvent0 = null;
//                } else if (pendingEvent != null){
//                    dataEvent = new ArrayList<>();
//                    dataEvent.add(pendingEvent);
//                    pendingEvent = null;
//                } else {
//                    try {
//                        dataEvent = eventQueue0.poll(5, TimeUnit.SECONDS);
//                        if(null == dataEvent){
//                            dataEvent = new ArrayList<>();
//                        }
//                        Optional.ofNullable(eventQueue.poll(500, TimeUnit.MILLISECONDS)).ifPresent(dataEvent::add);
//                    } catch (InterruptedException e) {
//                        break;
//                    }
//                    isPending.compareAndSet(true, false);
//                }
//
//                if (null != dataEvent && !dataEvent.isEmpty()) {
//                    for (TapdataEvent tapEvent : dataEvent) {
//                        TapEvent event = null;
//                        if (!isPending.get()) {
//                            TapCodecsFilterManager codecsFilterManager = getConnectorNode().getCodecsFilterManager();
//                            event = tapEvent.getTapEvent();
//                            tapRecordToTapValue(event, codecsFilterManager);
//                        }
//                        if (!offer(tapEvent)) {
//                            if (null == pendingEvent0 ) pendingEvent0 = new ArrayList<>();
//                            pendingEvent0.add(tapEvent);
//                            continue;
//                        }
//                        Optional.ofNullable(getSnapshotProgressManager())
//                                .ifPresent(s -> s.incrementEdgeFinishNumber(TapEventUtil.getTableId(tapEvent.getTapEvent())));
//                    }
//                }
//            } catch (Throwable e) {
//                errorHandle(e, "start source consumer failed: " + e.getMessage());
//                break;
//            }
//        }
//    }
//    public void enqueue(List<TapdataEvent> tapdataEvent) {
//        try {
//            while (isRunning()) {
//                //LinkedBlockingQueue<TapdataEvent> queue = queueMap.computeIfAbsent(Thread.currentThread().getId() + "_" + Thread.currentThread().getName(), key -> new LinkedBlockingQueue<>(sourceQueueCapacity));
//                if (eventQueue0.offer(tapdataEvent, 3, TimeUnit.SECONDS)) {
//                    break;
//                }
//            }
//        }
//        catch (Throwable throwable) {
//            throw new NodeException(throwable).context(getDataProcessorContext()).events(tapdataEvent.stream().map(TapdataEvent::getTapEvent).collect(Collectors.toList()));
//        }
//    }

}
