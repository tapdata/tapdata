package com.tapdata.validator;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.*;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.Processor;
import io.tapdata.debug.DebugConstant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public abstract class AbstractDataValidator {

	protected Logger logger = LogManager.getLogger(getClass());

	protected static final long DEFAUL_VALIDATE_INTERVAL_SEC = 10;

	protected Job job;

	private ClientMongoOperator metaMongoOperator;

	private Random random = new Random(100);

	protected ValidateDataSource validateDataSource;

	private Connections sourceConnection;

	private Connections targetConnection;

	private Map<String, DataValidateResult> validateResultMap;

	private DataValidateStats validateStats;

	private List<Processor> processors;

	private AtomicLong total;
	private AtomicLong error;
	private AtomicLong success;

	private ScheduledExecutorService scheduled = new ScheduledThreadPoolExecutor(1);

	protected AbstractDataValidator(Job job, Connections sourceConnection, Connections targetConnection, ClientMongoOperator metaMongoOperator, List<Processor> processors) {
		this.job = job;
		this.metaMongoOperator = metaMongoOperator;

		this.sourceConnection = sourceConnection;

		this.targetConnection = targetConnection;

		this.processors = processors;

		Stats stats = job.getStats();
		if (stats == null) {
			stats = new Stats();
			job.setStats(stats);
		}

		Long total = stats.getValidate_stats().getOrDefault("total", 0l);
		this.total = new AtomicLong(total);

		Long success = stats.getValidate_stats().getOrDefault("success", 0l);
		this.success = new AtomicLong(success);

		Long error = stats.getValidate_stats().getOrDefault("error", 0l);
		this.error = new AtomicLong(error);

		initValidateStats(job, sourceConnection, targetConnection);
	}

	private void initValidateStats(Job job, Connections sourceConnection, Connections targetConnection) {
		List<DataValidateResult> validateResults = new ArrayList<>();
		validateResultMap = new HashMap<>();
		List<Mapping> mappings = job.getMappings();
		StringBuilder sourceTables = new StringBuilder();
		StringBuilder targetTables = new StringBuilder();
		for (Mapping mapping : mappings) {
			String fromTable = mapping.getFrom_table();
			sourceTables.append(fromTable).append(",");
			String toTable = mapping.getTo_table();
			targetTables.append(toTable).append(",");

			DataValidateResult validateResult = new DataValidateResult();
			validateResult.setSourceTableName(fromTable);
			validateResult.setTargetTableName(toTable);

			StringBuilder srcPKs = new StringBuilder();
			StringBuilder targetPKs = new StringBuilder();
			String relationship = mapping.getRelationship();
			if (ConnectorConstant.RELATIONSHIP_MANY_ONE.equals(relationship)) {
				List<Map<String, String>> matchCondition = mapping.getMatch_condition();
				if (CollectionUtils.isEmpty(matchCondition)) {
					logger.info("Table {} does not have pk, cannot be validated.", fromTable);
					continue;
				}
				for (Map<String, String> map : matchCondition) {
					for (Map.Entry<String, String> entry : map.entrySet()) {
						srcPKs.append(entry.getKey()).append(",");
						targetPKs.append(entry.getValue()).append(",");
					}
				}
			} else {
				List<Map<String, String>> joinCondition = mapping.getJoin_condition();
				if (CollectionUtils.isEmpty(joinCondition)) {
					logger.info("Table {} does not have pk, cannot be validated.", fromTable);
					continue;
				}
				for (Map<String, String> map : joinCondition) {
					for (Map.Entry<String, String> entry : map.entrySet()) {
						srcPKs.append(entry.getKey()).append(",");
						targetPKs.append(entry.getValue()).append(",");
					}
				}
			}

			validateResult.setSourceTablePK(srcPKs.toString());
			validateResult.setTargetTablePK(targetPKs.toString());

			validateResult.setFieldProcesses(mapping.getFields_process());
			validateResult.setScript(mapping.getScript());

			validateResults.add(validateResult);
			validateResultMap.put(fromTable + "." + toTable, validateResult);

		}

		validateStats = new DataValidateStats();
		validateStats.setJodId(job.getId());
		validateStats.setJobName(job.getName());
		Map<String, Object> sourceInfo = new HashMap<>();
		sourceInfo.put("name", sourceConnection.getName());
		sourceInfo.put("database", sourceConnection.getDatabase_name());
		sourceInfo.put("host", sourceConnection.getDatabase_host() + ":" + sourceConnection.getDatabase_port());
		sourceInfo.put("tables", sourceTables.toString());
		validateStats.setSourceInfo(sourceInfo);

		Map<String, Object> targetInfo = new HashMap<>();
		targetInfo.put("name", targetConnection.getName());
		targetInfo.put("database", targetConnection.getDatabase_name());
		targetInfo.put("host", targetConnection.getDatabase_host() + ":" + targetConnection.getDatabase_port());
		targetInfo.put("tables", targetTables.toString());
		validateStats.setTargetInfo(targetInfo);
		validateStats.setValidateResult(validateResults);
	}

	protected void validate(ValidateData data) {

		if (data == null) {
			return;
		}

		Map<String, Object> sourceRow = data.getSourceRow();
		Map<String, Object> targetRow = data.getTargetRow();
		String fromTable = data.getFromTable();
		if (MapUtils.isEmpty(sourceRow) || MapUtils.isEmpty(targetRow)) {
			return;
		}

		if (CollectionUtils.isNotEmpty(processors)) {
			// construct message for processors
			MessageEntity message = new MessageEntity(ConnectorConstant.MESSAGE_OPERATION_INSERT, sourceRow, fromTable);

//            for (Processor processor : processors) {
//                processor.process(message);
//            }
		}

		if (MapUtils.isNotEmpty(sourceRow) && MapUtils.isNotEmpty(targetRow)) {

			Map<String, Object> finalSourceRow = sourceRow;
			targetRow.keySet().removeIf(e -> !finalSourceRow.containsKey(e));

		}

		SortedMap<String, Object> sourceSortedMap = new TreeMap<>(sourceRow);
		SortedMap<String, Object> targetSortedMap = new TreeMap<>(targetRow);

		int sourceHashCode = sourceSortedMap.toString().hashCode();
		int targetHashCode = targetSortedMap.toString().hashCode();


		if (sourceHashCode != targetHashCode) {
			data.setSecondValidate(true);
			validateError(data);
		} else {
			validatePassed(data);
		}
	}

	private void validatePassed(ValidateData data) {

		boolean secondValidate = data.isSecondValidate();
		if (secondValidate) {

			InconsistentData inconsistentData = data.getInconsistentData();
			retryValidatePassed(inconsistentData);

		} else {

			String fromTable = data.getFromTable();
			String toTable = data.getToTable();
			DataValidateResult validateResult = validateResultMap.get(fromTable + "." + toTable);
			validateResult.increaseCount(1, 1, 0);

			long success = this.success.incrementAndGet();
			long total = this.total.incrementAndGet();

			Stats stats = job.getStats();
			Map<String, Long> validateStats = stats.getValidate_stats();

			validateStats.put("success", success);
			validateStats.put("total", total);

		}
	}

	private void validateError(ValidateData data) {
//        boolean secondValidate = data.isSecondValidate();
//        if (secondValidate) {
//
		String fromTable = data.getFromTable();
		String toTable = data.getToTable();
		DataValidateResult validateResult = validateResultMap.get(fromTable + "." + toTable);
		validateResult.increaseCount(1, 0, 1);
//
//            InconsistentData inconsistentData = data.getInconsistentData();
//            retryValidateError(inconsistentData);
//
//        } else {

		Object event = data.getEvent();
		Map<String, Object> sourceRow = data.getSourceRow();
		Map<String, Object> targetRow = data.getTargetRow();
		inconsistentData(event, sourceRow, targetRow);
//        }
	}

	public void start() {

		try {

			String databaseType = sourceConnection.getDatabase_type();
			ValidatorConstant.ValidateClassEnum validateClassEnum = ValidatorConstant.ValidateClassEnum.fromString(databaseType);

			if (validateClassEnum != null) {
				String validateClassName = validateClassEnum.getValidateClassName();
				Class<? extends ValidateDataSource> validateDataSourceClass = (Class<? extends ValidateDataSource>) getClass().getClassLoader().loadClass(validateClassName);
				validateDataSource = validateDataSourceClass.newInstance();
				validateDataSource.initialize(sourceConnection, job.getMappings());
			}

			setThreadContext(job);
			logger.info(TapLog.TRAN_LOG_0013.getMsg(), job.getSampleRate());
			this.generateValidateData(this::validate);

		} catch (Exception e) {
			logger.error("Start data validator failed {}", e.getMessage(), e);
		}
	}

	public void stop() {

		scheduled.shutdown();

		validateDataSource.releaseResource();

	}

	protected boolean needToValidate() {
		return random.nextDouble() * 100 <= job.getSampleRate();
	}

	private void retryValidateError(InconsistentData inconsistentData) {

		try {
			String id = inconsistentData.getId();
			long error = this.error.incrementAndGet();
			long total = this.total.incrementAndGet();

			Stats stats = job.getStats();
			Map<String, Long> validateStats = stats.getValidate_stats();

			validateStats.put("error", error);
			validateStats.put("total", total);

			Query query = new Query(where("id").is(id));
			Update update = new Update().set("validateResult", InconsistentData.VALIDATE_RESULT_ERROR);

			metaMongoOperator.update(query, update, ConnectorConstant.INCONSISTENT_DATA_COLLECTION);
		} catch (Exception e) {
			logger.error("Validate error {}", e, e);
		}
	}

	private void retryValidatePassed(InconsistentData inconsistentData) {
		try {
			String id = inconsistentData.getId();
			long success = this.success.incrementAndGet();
			long total = this.total.incrementAndGet();

			Stats stats = job.getStats();
			Map<String, Long> validateStats = stats.getValidate_stats();

			validateStats.put("success", success);
			validateStats.put("total", total);

			Query query = new Query(where("id").is(id));

			metaMongoOperator.delete(query, ConnectorConstant.INCONSISTENT_DATA_COLLECTION);
		} catch (Exception e) {
			logger.error("Validate error {}", e, e);
		}
	}

	private void retryValidate() {

		Query query = new Query(
				new Criteria().andOperator(
						where("validateResult").is(InconsistentData.VALIDATE_RESULT_RETRY),
						where("nextValidateTime").lte(Double.valueOf(String.valueOf(System.currentTimeMillis()))),
						where("verificationJobId").is(job.getId())

				)
		);

		try {
			List<InconsistentData> inconsistentDataList = metaMongoOperator.find(query, ConnectorConstant.INCONSISTENT_DATA_COLLECTION, InconsistentData.class);

			for (InconsistentData inconsistentData : inconsistentDataList) {

				try {
					Object eventRecord = inconsistentData.getEventRecord();

					ValidateData validateData = getValidateDataByEvent(eventRecord);
					validateData.setSecondValidate(true);
					validateData.setInconsistentData(inconsistentData);

					validate(validateData);
				} catch (Exception e) {
					logger.error("Retry validate fail ", e);
				}

			}
		} catch (Exception e) {
			logger.error("Retry validate fail", e);
		}
	}

	private void inconsistentData(Object eventRecord, Map<String, Object> sourceRecord, Map<String, Object> targetRecord) {
		String jobId = job.getId();
		Map<String, Object> ts = null;
		ProgressRateStats progressRateStats = job.getProgressRateStats();
		if (progressRateStats != null) {
			ts = progressRateStats.getTs();
		}
		long interval = DEFAUL_VALIDATE_INTERVAL_SEC;
		if (MapUtils.isNotEmpty(ts)) {
			Long lag = (Long) ts.get("lag");
			if (lag != null) {
				interval = lag;
			}
		}

		long nextValidateTime = System.currentTimeMillis() + interval * 1000;

		InconsistentData data = new InconsistentData(jobId, sourceRecord, nextValidateTime, eventRecord, targetRecord);

		metaMongoOperator.insertOne(data, ConnectorConstant.INCONSISTENT_DATA_COLLECTION);
	}

	abstract void generateValidateData(Consumer<ValidateData> validateConsumer);

	abstract ValidateData getValidateDataByEvent(Object event);

	public abstract boolean running();

	public abstract boolean validate();

	public DataValidateStats getValidateStats() {
		return validateStats;
	}

	protected void setThreadContext(Job job) {
		ThreadContext.clearAll();

		ThreadContext.put("userId", job.getUser_id());
		ThreadContext.put("jobId", job.getId());
		ThreadContext.put("jobName", job.getName());
		ThreadContext.put("app", ConnectorConstant.WORKER_TYPE_CONNECTOR);
		if (StringUtils.isNotBlank(job.getDataFlowId())) {
			ThreadContext.put(DebugConstant.SUB_DATAFLOW_ID, job.getDataFlowId());
		}
	}
}
