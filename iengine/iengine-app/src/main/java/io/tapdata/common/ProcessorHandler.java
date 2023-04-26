package io.tapdata.common;

import com.tapdata.cache.ICacheService;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ConnectorContext;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.DataFlowUtil;
import com.tapdata.constant.Graph;
import com.tapdata.constant.ListUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MessageUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.DataRulesProcessor;
import com.tapdata.processor.Processor;
import com.tapdata.processor.ProcessorException;
import com.tapdata.processor.ProcessorUtil;
import com.tapdata.processor.dataflow.DataFlowProcessor;
import com.tapdata.processor.dataflow.ProcessorContext;
import io.tapdata.debug.DebugException;
import io.tapdata.debug.DebugProcessor;
import io.tapdata.debug.DebugUtil;
import io.tapdata.exception.SourceException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 */
public class ProcessorHandler {

	private Logger logger = LogManager.getLogger(getClass());

	private ConnectorContext context;

	private Graph<Stage> jobStageGraph;

	private DebugProcessor debugProcessor;

	private Map<String, DataFlowProcessor> processorMap;

	private Map<String, Stage> sourceStageTableNameMap;

	private Map<String, Stage> sourceStageIdMap;

	private Map<String, Mapping> srcTARSTGMappingMap;

	private Map<String, Mapping> srcTableMappingMap;

	private ExecutorService executorService;

	private SettingService settingService;

	private DataRulesProcessor dataRulesProcessor;

	private Object previousBatchLastOffset = null;

	private boolean hasMultiTargetStage = false;

	private ProcessorContext processorContext;

	private Consumer<List<MessageEntity>> pushMessages2Queue;

	public ProcessorHandler(
			ConnectorContext context,
			DebugProcessor debugProcessor,
			SettingService settingService,
			Consumer<List<MessageEntity>> pushMessages2Queue,
			ICacheService cacheService) {
		this.context = context;
		this.debugProcessor = debugProcessor;

		ClientMongoOperator clientMongoOperator = context.getClientMongoOpertor();
		List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
		this.processorContext = new ProcessorContext(
				context.getJobSourceConn(),
				context.getJobTargetConn(),
				context.getJob(),
				context.getClientMongoOpertor(),
				javaScriptFunctions,
				this::handle,
				cacheService
		);
		List<Stage> stages = context.getJob().getStages();
		if (CollectionUtils.isNotEmpty(stages)) {

			initDataFlowProcess(context, stages);

			int processorConcurrency = context.getJob().getProcessorConcurrency();
			if (processorConcurrency > 1) {
				executorService = new ThreadPoolExecutor(0, processorConcurrency,
						5L, TimeUnit.SECONDS,
						new SynchronousQueue<>(),
						(runnable) -> new Thread(runnable, "processor-thread-[" + context.getJob().getId() + "]-" + context.getJob().getName()),
						new ThreadPoolExecutor.CallerRunsPolicy());

			}

			for (Stage stage : stages) {
				List<String> outputLanes = stage.getOutputLanes();
				if (CollectionUtils.isNotEmpty(outputLanes) && outputLanes.size() > 1) {
					hasMultiTargetStage = true;
					break;
				}
			}
		}

		this.settingService = settingService;
		this.pushMessages2Queue = pushMessages2Queue;

		try {
			this.dataRulesProcessor = new DataRulesProcessor();
			this.dataRulesProcessor.initialize(processorContext, null);
		} catch (Exception e) {
			throw new SourceException(String.format("Initial data rule process failed %s", e.getMessage()), e, true);
		}
	}

	public void stop() {
		logger.info("Stopping processor handler.");
		if (executorService != null && !executorService.isShutdown()) {
			executorService.shutdownNow();
		}
		if (MapUtils.isNotEmpty(processorMap)) {
			for (DataFlowProcessor dataFlowProcessor : processorMap.values()) {
				if (dataFlowProcessor != null) {
					dataFlowProcessor.stop();
				}
			}
		}
		if (processorContext != null) {
			processorContext.destroy();
		}
		logger.info("Completed stop processor handler.");
	}

	private void initDataFlowProcess(ConnectorContext context, List<Stage> stages) {
		sourceStageTableNameMap = new LinkedHashMap<>();
		sourceStageIdMap = new LinkedHashMap<>();

		List<Mapping> mappings = context.getJob().getMappings();
		int loopCounter = 0;
		for (Mapping mapping : mappings) {
			if (!context.isRunning()) {
				break;
			}
			String fromTable = mapping.getFrom_table();

			List<Stage> mappingStages = mapping.getStages();
			Stage sourceStage = mappingStages.get(0);
			sourceStageTableNameMap.put(fromTable, sourceStage);

			Stage lastStage = mappingStages.get(mappingStages.size() - 1);

			Mapping mappingClone = SerializationUtils.clone(mapping);
			List<Map<String, String>> joinConditions = Mapping.reverseConditionMapKeyValue(mappingClone.getJoin_condition());
			mappingClone.setJoin_condition(joinConditions);
			List<Map<String, String>> matchConditions = Mapping.reverseConditionMapKeyValue(mappingClone.getMatch_condition());
			mappingClone.setMatch_condition(matchConditions);
			Mapping.initMappingForFieldProcess(mappingClone, context.getJob().getMapping_template());

			if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(context.getJob().getMapping_template())) {
				if (srcTableMappingMap == null) {
					srcTableMappingMap = new HashMap<>();
				}

				srcTableMappingMap.put(mappingClone.getFrom_table(), mappingClone);
			} else {
				if (srcTARSTGMappingMap == null) {
					srcTARSTGMappingMap = new LinkedHashMap<>();
				}
				srcTARSTGMappingMap.put(sourceStage.getId() + lastStage.getId(), mappingClone);
			}
			if ((++loopCounter) % ConnectorConstant.LOOP_BATCH_SIZE == 0) {
				logger.info("Init dataflow process progress: " + loopCounter + "/" + mappings.size());
			}
		}

		if (CollectionUtils.isNotEmpty(context.getJob().getStages())) {
			for (Stage stage : context.getJob().getStages()) {
				sourceStageIdMap.put(stage.getId(), stage);
			}
		}

		this.jobStageGraph = DataFlowUtil.buildDataFlowGrah(stages);
		List<DataFlowProcessor> dataFlowProcessors = ProcessorUtil.stagesConvertToProcessor(
				stages,
				processorContext
		);

		if (CollectionUtils.isNotEmpty(dataFlowProcessors)) {
			processorMap = new HashMap<>();
			for (DataFlowProcessor dataFlowProcessor : dataFlowProcessors) {
				processorMap.put(dataFlowProcessor.getStage().getId(), dataFlowProcessor);
			}
		}
	}

	public void handle(List<MessageEntity> msgs) {
		try {
			if (jobStageGraph != null) {

				if (CollectionUtils.isEmpty(msgs)) {
					return;
				}

				int processorConcurrency = context.getJob().getProcessorConcurrency();
				if (processorConcurrency > 1) {

					int size = msgs.size();
					int subBatchSize = size / processorConcurrency;
					subBatchSize = size % processorConcurrency > 0 ? subBatchSize + 1 : subBatchSize;

					// 多线程处理
					CountDownLatch countDownLatch = new CountDownLatch(processorConcurrency);
					List<List<MessageEntity>> processedBatch = new ArrayList<>(Collections.nCopies(processorConcurrency, null));
					for (int i = 0; i < processorConcurrency; i++) {
						int threadNo = i;
						int fromIndex = threadNo * subBatchSize;

						if (!executorService.isShutdown()) {
							int finalSubBatchSize = subBatchSize;
							executorService.submit(() -> {
								try {
									Log4jUtil.setThreadContext(context.getJob());
									if (fromIndex < size) {

										List<MessageEntity> subMsgs = msgs.stream().skip(fromIndex).limit(finalSubBatchSize).collect(Collectors.toList());
										List<MessageEntity> processedMSGs = process(subMsgs);
										processedBatch.set(threadNo, processedMSGs);
									}
								} catch (Exception e) {
									throw new SourceException(e, true);
								} finally {
									countDownLatch.countDown();
								}

							});
						}
					}

					// 等待线程处理完成
					while (context.isRunning()) {
						if (countDownLatch.await(10, TimeUnit.SECONDS)) {
							msgs.clear();
							for (List<MessageEntity> batch : processedBatch) {
								if (CollectionUtils.isNotEmpty(batch)) {
									msgs.addAll(batch);
								}
							}

							break;
						}
					}

				} else {
					List<MessageEntity> processedMSGs = process(msgs);
					msgs.clear();
					msgs.addAll(processedMSGs);
				}

				if (CollectionUtils.isNotEmpty(msgs)) {
					dataRulesProcessor.process(msgs);
				}

				if (hasMultiTargetStage && CollectionUtils.isNotEmpty(msgs)) {

					int msgSize = msgs.size();
					if (previousBatchLastOffset != null) {
						for (int i = 0; i < msgSize; i++) {
							MessageEntity messageEntity = msgs.get(i);

							if (i == msgSize - 1) {
								break;
							}

							messageEntity.setOffset(previousBatchLastOffset);
						}
					}

					previousBatchLastOffset = msgs.get(msgSize - 1).getOffset();
				}

			} else {
				List<Processor> processors = context.getProcessors();
				if (CollectionUtils.isNotEmpty(processors)) {
					for (Processor processor : processors) {
						if (!context.isRunning()) {
							break;
						}

						processor.process(msgs);
					}
				}
			}
		} catch (InterruptedException e) {

			return;

		} catch (Exception e) {
			throw new ProcessorException(e, true);
		}

		if (logger.isTraceEnabled()) {
			logger.trace("After processor processed messages left {}", msgs.size());
		}

		if (CollectionUtils.isNotEmpty(msgs)) {
			final List<MessageEntity> list = msgs.stream().filter(msg -> {
				final String op = msg.getOp();
				if (OperationType.COMMIT_OFFSET.getOp().equals(op)) {
					final Object offset = msg.getOffset();
					if (offset == null) {
						return false;
					}

					if (offset instanceof TapdataOffset) {
						TapdataOffset tapdataOffset = (TapdataOffset) offset;
						return tapdataOffset.getOffset() != null;
					} else {
						return true;
					}
				} else {
					return true;
				}
			}).collect(Collectors.toList());
			if (CollectionUtils.isNotEmpty(list)) {
				pushMessages2Queue.accept(list);
			}
		}
	}

	private List<MessageEntity> process(List<MessageEntity> msgs) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		List<MessageEntity> processedMSGs = new ArrayList<>();

		if (CollectionUtils.isNotEmpty(context.getJob().getMappings())) {
			mappingHandle(msgs, processedMSGs);
		} else {
			List<Stage> stages = context.getJob().getStages();
			if (CollectionUtils.isEmpty(stages)) {
				throw new SourceException("Stages cannot be empty.", true);
			}
			List<MessageEntity> processResult = dispatchForProcessor(msgs, stages.get(0));
			if (CollectionUtils.isNotEmpty(processResult)) {
				processedMSGs.addAll(processResult);
			}
		}
		return processedMSGs;
	}

	private List<MessageEntity> dispatchForProcessor(List<MessageEntity> sameTableMSGs, Stage stage) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		List<MessageEntity> processedMSGs = new ArrayList<>();
		Map<Stage, List<Stage>> paths = jobStageGraph.getPaths();
		statsStageThroughput(sameTableMSGs, stage, false);
		List<Stage> childStages = paths.get(stage);
		if (CollectionUtils.isNotEmpty(childStages)) {

			// source debug
			if (context.getJob().isDebug()) {
				try {
					DebugUtil.handleStage(stage, Stage.SOURCE_STAGE);
					debugProcessor.debugProcess(stage, sameTableMSGs);
				} catch (DebugException e) {
					logger.error("Debug process error, message: {}", e.getMessage(), e);
				}
			}

			sameTableMSGs = dataFlowProcess(sameTableMSGs, stage, childStages);
			processedMSGs.addAll(sameTableMSGs);
		}

		return processedMSGs;
	}

	private List<MessageEntity> dataFlowProcess(List<MessageEntity> msgs, Stage sourceStage, List<Stage> stages) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		if (CollectionUtils.isEmpty(msgs)) {
			return msgs;
		}
		long startProcessTs = System.currentTimeMillis();
		List<MessageEntity> processedMSGs = new ArrayList<>();
		List<List<MessageEntity>> stagesMSGs = new ArrayList<>();
		stagesMSGs.add(msgs);
		int size = stages.size();

		if (size > 1) {
			for (int i = 1; i < size; i++) {
				List<MessageEntity> newMSGs = new ArrayList<>();
				ListUtil.cloneableCloneList(msgs, newMSGs);
				stagesMSGs.add(newMSGs);
				long endCopyMsgsTs = System.currentTimeMillis();
				long spentTs = endCopyMsgsTs - startProcessTs;
				if (logger.isDebugEnabled()) {
					logger.debug("Clone {} messages for stage {}, spent {}ms.", newMSGs.size(), stages.get(i).getName(), spentTs);
				}
			}
			long endCopyMsgsTs = System.currentTimeMillis();
			long spentTs = endCopyMsgsTs - startProcessTs;
			if (logger.isDebugEnabled()) {
				logger.debug("Clone all messages  spent {}ms.", (spentTs));
			}
		}
		Map<Stage, List<Stage>> paths = jobStageGraph.getPaths();
		for (int i = 0; i < size; i++) {

			if (!context.isRunning()) {
				break;
			}

			Stage stage = stages.get(i);
			String stageId = stage.getId();

			msgs = stagesMSGs.get(i);

			statsStageThroughput(msgs, stage, true);

			if (MapUtils.isNotEmpty(processorMap) && processorMap.containsKey(stageId)) {
				long startStageProcessTs = System.currentTimeMillis();

				int originalSize = msgs.size();
				DataFlowProcessor dataFlowProcessor = processorMap.get(stageId);
				List<MessageEntity> processedTmpMsgs = new ArrayList<>();
				MessageUtil.dispatcherMessage(msgs, false, (messageEntities) -> {
					List<MessageEntity> process = dataFlowProcessor.process(messageEntities);
					if (CollectionUtils.isNotEmpty(process)) {
						processedTmpMsgs.addAll(process);
					}
				}, (msg) -> {
					processedTmpMsgs.add(msg);
				});

				msgs = processedTmpMsgs;

				long endProcessTs = System.currentTimeMillis();
				if (logger.isDebugEnabled()) {
					logger.debug("Processor {} processed {} messages of stage {}, remaining {} messages, spent {}ms", stage.getName(), originalSize, sourceStage.getName(), msgs.size(), (endProcessTs - startStageProcessTs));
				}

				context.getJob().getStats().statsStageTransTime(startStageProcessTs, endProcessTs, stageId);

				// processor debug
				if (context.getJob().isDebug()) {
					try {
						DebugUtil.handleStage(stage, Stage.SOURCE_STAGE);
						debugProcessor.debugProcess(stage, msgs);
					} catch (DebugException e) {
						logger.error("Debug process error, message: {}", e.getMessage(), e);
					}
				}

				if (paths.containsKey(stage)) {
					List<Stage> childStages = paths.get(stage);
					String type = stage.getType();
					if (!DataFlowStageUtil.isDataStage(type)) {
						statsStageThroughput(msgs, stage, false);
					}
					msgs = dataFlowProcess(msgs, stage, childStages);
				}
			}

			List<String> inputLanes = stage.getInputLanes();
			if (CollectionUtils.isNotEmpty(inputLanes) && DataFlowStageUtil.isDataStage(stage.getType())) {
				for (MessageEntity msg : msgs) {
					String targetStageId = stageId.intern();
					msg.setTargetStageId(targetStageId);
					String sourceStageId = msg.getSourceStageId();

					Mapping mapping = null;
					if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(context.getJob().getMapping_template())) {
						if (MapUtils.isNotEmpty(srcTableMappingMap)) {
							mapping = srcTableMappingMap.get(msg.getTableName());
						}
					} else {
						if (MapUtils.isNotEmpty(srcTARSTGMappingMap)) {
							mapping = srcTARSTGMappingMap.get(sourceStageId + targetStageId);
						}
					}

					msg.setMapping(mapping);
				}
			}

			if (CollectionUtils.isNotEmpty(msgs)) {
				processedMSGs.addAll(msgs);
			}
		}

		return processedMSGs;
	}

	private void mappingHandle(List<MessageEntity> msgs, List<MessageEntity> processedMSGs) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		int msgSize = msgs.size();
		List<MessageEntity> sameTableMsgs = new ArrayList<>(msgSize);
		String preStageId = null;
		for (int i = 0; i < msgSize; i++) {

			if (!context.isRunning()) {
				break;
			}

			MessageEntity msg = msgs.get(i);

			if (!OperationType.CREATE_INDEX.getOp().equalsIgnoreCase(msg.getOp()) && (OperationType.isDdl(msg.getOp()) || OperationType.isNotify(msg.getOp()))) {
				processedMSGs.add(msg);
				continue;
			}
			Stage stage = null;
			String tableName = msg.getTableName();
			if (StringUtils.isNotBlank(msg.getProcessorStageId())) {
				stage = sourceStageIdMap.get(msg.getProcessorStageId());
			} else {
				stage = sourceStageTableNameMap.get(tableName);
				if (stage != null) {
					msg.setSourceStageId(stage.getId().intern());
				}
			}
			if (stage == null) {
				throw new SourceException("Cannot find stage info by msg " + msg.toString(), true);
			}
			String stageId = stage.getId();

			preStageId = StringUtils.isBlank(preStageId) ? stageId : preStageId;

			// batch process all same source table message
			if (StringUtils.isNotEmpty(preStageId) && !preStageId.equals(stageId)) {
				Stage preMSGStage = sourceStageIdMap.get(preStageId);
				List<MessageEntity> processResult = dispatchForProcessor(sameTableMsgs, preMSGStage);
				processedMSGs.addAll(processResult);
				sameTableMsgs.clear();
			}


			preStageId = stageId;
			sameTableMsgs.add(msg);
		}
		if (CollectionUtils.isNotEmpty(sameTableMsgs)) {
			Stage stage = sourceStageIdMap.get(preStageId);
			List<MessageEntity> processResult = dispatchForProcessor(sameTableMsgs, stage);
			processedMSGs.addAll(processResult);
			sameTableMsgs.clear();
		}
	}

	public void statsStageThroughput(List<MessageEntity> msgs, Stage stage, boolean input) {
		if (stage != null) {

			List<StageRuntimeStats> stageRuntimeStats = context.getJob().getStats().getStageRuntimeStats();

			if (CollectionUtils.isEmpty(stageRuntimeStats)) {
				return;
			}

			String stageId = stage.getId();

			for (StageRuntimeStats stageRuntimeStat : stageRuntimeStats) {
				if (stageId.equals(stageRuntimeStat.getStageId())) {
					long messageDataSize = 0;
					//long messageDataSize = MessageUtil.processableMessageDataSize(msgs);
					int messageCount = msgs.size();
					if (input) {
						stageRuntimeStat.incrementInput(messageCount, messageDataSize);
					} else {
						stageRuntimeStat.incrementOuput(messageCount, messageDataSize);
					}
					break;
				}
			}
		}
	}

	public static void main(String[] args) {
		for (int j = 0; j < 20; j++) {

			long totalMsgSize = 0L;
			long totalProcessed = 0L;
			long createdThread = 0L;

			int totalThread = RandomUtils.nextInt(1, 64);
			int totalMsg = RandomUtils.nextInt(1, 9999999);
			List<MessageEntity> msgs = new ArrayList<>(Collections.nCopies(totalMsg, null));
			int size = msgs.size();
			totalMsgSize += size;

			int subBatchSize = size / totalThread;
			subBatchSize = size % totalThread > 0 ? subBatchSize + 1 : subBatchSize;

			for (int i = 0; i < totalThread; i++) {
				int threadNo = i;
				int fromIndex = threadNo * subBatchSize;
				int toIndex = (threadNo + 1) * subBatchSize;
				createdThread++;
				List<MessageEntity> subMsgs = null;
				if (fromIndex < size) {
					subMsgs = msgs.subList(fromIndex, toIndex > size ? size : toIndex);
					totalProcessed += subMsgs.size();
				}
			}

			System.out.println("==================");
			System.out.println("totalMsgSize: " + totalMsgSize);
			System.out.println("totalProcessed: " + totalProcessed);
			System.out.println("createdThread: " + createdThread);

			if (totalProcessed != totalMsgSize) {
				System.out.println(">>>>>>>>>>>>>> Failed <<<<<<<<<<<<<<<");
			}
		}

	}
}
