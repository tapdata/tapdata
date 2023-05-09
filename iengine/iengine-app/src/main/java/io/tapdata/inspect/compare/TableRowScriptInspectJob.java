package io.tapdata.inspect.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.entity.inspect.InspectTask;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;
import com.tapdata.processor.constant.JSEngineEnum;
import io.tapdata.ConverterProvider;
import io.tapdata.inspect.InspectTaskContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Query;

import javax.script.Invocable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author lhm
 * @createTime 2020-12-22 15:55
 */
public class TableRowScriptInspectJob extends InspectTableRowJob {
	private Logger logger = LogManager.getLogger(TableRowScriptInspectJob.class);

	// 统计变量
	protected long current = 0;
	protected long both = 0;
	protected long rowPassed = 0;
	protected long rowField = 0;
	protected long startTime = System.currentTimeMillis() / 1000;
	long max = 0L;

	public TableRowScriptInspectJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
	}

	@Override
	protected void doRun() {
		int retry = 0;
		while (retry < 4) {
			try {
				compare(inspectTask, source, target, stats, (inspectResultStats, inspectDetails) -> {
					progressUpdateCallback.progress(inspectTask, stats, inspectDetails);
				});
				break;
			} catch (Exception e) {
				if (retry >= 3) {
					logger.error(String.format("Failed to compare the count of rows in table %s.%s and table %s.%s, the taskId is %s",
							source.getName(), inspectTask.getSource().getTable(),
							target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()), e);

					stats.setEnd(new Date());
					stats.setStatus(InspectStatus.ERROR.getCode());
					stats.setResult("failed");
					stats.setErrorMsg(e.getMessage());
					break;
				}
				retry++;
				stats.setErrorMsg(String.format("Check has an exception and is trying again..., The number of retries: %s", retry));
				stats.setStatus(InspectStatus.RUNNING.getCode());
				stats.setEnd(new Date());
				stats.setResult("failed");
				progressUpdateCallback.progress(inspectTask, stats, null);
				logger.error(String.format("Check has an exception and is trying again..., The number of retries: %s", retry));
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException interruptedException) {
					break;
				}
			}
		}
	}

	private void compare(InspectTask inspectTask, Connections source, Connections target, InspectResultStats stats, TableRowContentInspectJob.CompareProgress compareProgress) throws InstantiationException, IllegalAccessException, ClassNotFoundException, Exception {

		ScriptConnection sourceConnection = ScriptUtil.initScriptConnection(source);
		ScriptConnection targetConnection = ScriptUtil.initScriptConnection(target);

//        try {
//            String sourceZone = TimeZoneUtil.getZoneIdByDatabaseType(source);
//            source.setZoneId(ZoneId.of(sourceZone));
//            source.initCustomTimeZone();
//        } catch (Exception e) {
//        }
//        try {
//            String targetZone = TimeZoneUtil.getZoneIdByDatabaseType(target);
//            target.setZoneId(ZoneId.of(targetZone));
//            target.initCustomTimeZone();
//        } catch (Exception e) {
//        }

		if (inspectTask.getBatchSize() > 0) {
			batchSize = inspectTask.getBatchSize();
		}

		source.setSchema(null);
		target.setSchema(null);
		if (logger.isDebugEnabled()) {
			try {
				logger.debug(JSONUtil.obj2JsonPretty(source));
				logger.debug(JSONUtil.obj2JsonPretty(target));
			} catch (JsonProcessingException ignore) {
			}
		}

		List<JavaScriptFunctions> javaScriptFunctions = clientMongoOperator.find(new Query(where("type").ne("system")).with(Sort.by(Sort.Order.asc("last_update"))), ConnectorConstant.JAVASCRIPT_FUNCTION_COLLECTION, JavaScriptFunctions.class);
		Invocable scriptEngine = ScriptUtil.getScriptEngine(JSEngineEnum.GRAALVM_JS.getEngineName(), inspectTask.getScript(), javaScriptFunctions, clientMongoOperator, sourceConnection, targetConnection, null, logger);
		if (null == scriptEngine)
			throw new Exception(String.format("Script engine is null, task id: %s", inspectTask.getTaskId()));

		List<String> sourceKeys = getSortColumns(inspectTask.getSource().getSortColumn());
		List<String> targetKeys = getSortColumns(inspectTask.getTarget().getSortColumn());
		try (DiffDetailCursor diffDetailCursor = new DiffDetailCursor(inspectResultParentId, clientMongoOperator, sourceKeys, targetKeys)) {
			List<InspectDetail> inspectDetails = new ArrayList<>();
			long sourceTotal = 0;
			long targetTotal = 0;
			while (diffDetailCursor.next() && !Thread.interrupted()) {
				try (
						BaseResult<Map<String, Object>> sourceCursor = queryForCursor(source, null, inspectTask.getSource(), inspectTask.isFullMatch(), diffDetailCursor.getData());
						BaseResult<Map<String, Object>> targetCursor = queryForCursor(target, null, inspectTask.getTarget(), inspectTask.isFullMatch(), diffDetailCursor.getData())
				) {
					if (null == sourceCursor)
						throw new Exception(String.format("Query source result is null, table: %s, task id: %s", source.getCollection_name(), inspectTask.getTaskId()));
					if (null == targetCursor)
						throw new Exception(String.format("Query target result is null, table: %s, task id: %s", target.getCollection_name(), inspectTask.getTaskId()));

					sourceTotal += sourceCursor.getTotal();
					targetTotal += targetCursor.getTotal();
					if (diffDetailCursor.diffCounts() > 0) {
						// 差异数据数量
						max = diffDetailCursor.diffCounts();
					} else {
						// 全量校验，源和目标取最大数
						max = sourceTotal;
					}

					boolean moveSource = true, sourceHasNext = true;
					Map<String, Object> sourceRecord = null;

					while (moveSource) {
						if (moveSource) sourceHasNext = sourceCursor.hasNext();
						if (!sourceHasNext) break;

						current++;

						if (current % 5000 == 0) {
							double progress = getProgress();
							if (current % 20000 == 0) {
								logger.info("Compared " + current + ", total " + max + ", completed " + Math.round(progress * 100) + "%");
							}

							stats.setProgress(progress);
							stats.setCycles(current);
							stats.setBoth(both);
							stats.setSource_total(sourceTotal);
							stats.setTarget_total(targetTotal);
							stats.setRow_passed(rowPassed);
							stats.setRow_failed(rowField);

							stats.setSpeed(current / (System.currentTimeMillis() / 1000 - startTime + 1));

							compareProgress.update(stats, null);
						}

						if (moveSource) {
							if (sourceHasNext) {
								sourceRecord = sourceCursor.next();
							} else {
								moveSource = false;
								sourceRecord = Collections.emptyMap();
							}
						}
						Object o = scriptEngine.invokeFunction(ScriptUtil.SCRIPT_FUNCTION_NAME, sourceRecord);
						if (null == o) throw new Exception("Can't find the return value in execute script");
						Map<String, Object> map = (Map<String, Object>) o;
						String result = (String) map.getOrDefault("result", "");

						if (!StringUtils.isEmpty(result)) {
							if ("passed".equalsIgnoreCase(result)) {
								rowPassed++;
							} else {
								rowField++;
								Object data = map.getOrDefault("data", null);
								Map<String, Object> targetRecord = new HashMap<>();
								String message = (String) map.getOrDefault("message", null);
								targetRecord.put("data", data);
								recursionMap(targetRecord);

								InspectDetail detail = new InspectDetail();
								detail.setSource(sourceRecord);
								detail.setTarget(targetRecord);
								detail.setType("otherFields");
								detail.setMessage(message);
								inspectDetails.add(detail);
							}
						} else {
							rowField++;
							InspectDetail detail = new InspectDetail();
							detail.setSource(sourceRecord);
							detail.setType("otherFields");
							detail.setMessage("Execute script result is empty");
							inspectDetails.add(detail);
						}

						if (inspectDetails.size() > 50) {
							compareProgress.update(stats, inspectDetails);
							inspectDetails = new ArrayList<>();
						}
					} // end while data

					stats.setSource_total(sourceTotal);
					stats.setTarget_total(targetTotal);
				}
			} // end while diffDetailCursor

			stats.setCycles(current);
			stats.setBoth(both);
			stats.setCycles(current);
			stats.setRow_passed(rowPassed);
			stats.setRow_failed(rowField);
			stats.setSpeed(current / (System.currentTimeMillis() / 1000 - startTime + 1));
			stats.setSource_total(sourceTotal);
			stats.setTarget_total(targetTotal);
			stats.setEnd(new Date());
			stats.setProgress(1);
			stats.setStatus(InspectStatus.DONE.getCode());
			stats.setResult(rowField > 0 ? "failed" : "passed");
			compareProgress.update(stats, inspectDetails);

			logger.info("compare " + current);
		} catch (Throwable e) {
			logger.error("Advanced check failed", e);
			e.printStackTrace();
			stats.setEnd(new Date());
			stats.setResult("failed");
			stats.setStatus(InspectStatus.ERROR.getCode());
			stats.setErrorMsg(e.getMessage());
			compareProgress.update(stats, null);
		}

	}

	/**
	 * 递归转换_id,时间类型
	 *
	 * @param map 需要转换_id的数据
	 */
	private void recursionMap(Map<String, Object> map) {
		Set<String> keySet = map.keySet();
		for (String key : keySet) {
			if ("_id".equals(key)) {
				Object _id = map.get(key);
				if (_id instanceof ObjectId) {
					map.put(key, ((ObjectId) _id).toHexString());
				}
			}
			Object obj = map.get(key);
			if (obj instanceof Date) {
				Date date = (Date) obj;
				LocalDateTime localDateTime = date.toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime();
				DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				String format = dtf.format(localDateTime);
				map.put(key, format);
			}
			if (obj instanceof Map) {
				recursionMap((Map<String, Object>) obj);
			}
			if (obj instanceof List) {
				List<Object> objects = (List<Object>) obj;
				for (Object object : objects) {
					if (object instanceof Map) {
						recursionMap((Map<String, Object>) object);
					}
				}
			}
		}
	}

	private double getProgress() {
		return new BigDecimal(current)
				.divide(new BigDecimal(current > max ? current + 1 : max), 4, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	private BaseResult<Map<String, Object>> queryForCursor(Connections connections, ConverterProvider converterProvider, InspectDataSource inspectDataSource, boolean fullMatch, List<List<Object>> diffKeyValues) throws Exception {

		inspectDataSource.setDirection("DESC"); // force desc

		if (StringUtils.equalsAnyIgnoreCase(connections.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())) {
			return this.findMongo(connections, converterProvider, inspectDataSource, fullMatch, diffKeyValues);
		}

		if (rdbmsTypes.contains(connections.getDatabase_type())) {
			return this.findRdbms(connections, converterProvider, inspectDataSource, fullMatch, diffKeyValues);
		}

		return null;
	}

}
