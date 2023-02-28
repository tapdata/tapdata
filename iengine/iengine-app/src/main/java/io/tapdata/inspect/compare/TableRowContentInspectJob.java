package io.tapdata.inspect.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tapdata.constant.JSONUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.*;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/21 7:55 上午
 * @description
 */
public class TableRowContentInspectJob extends InspectTableRowJob {
	private final Logger logger = LogManager.getLogger(TableRowContentInspectJob.class);
	private final Gson gson = new GsonBuilder().serializeNulls().create();

	// 统计变量
	protected long current = 0;
	protected long both = 0;
	protected long sourceOnly = 0;
	protected long targetOnly = 0;
	protected long rowPassed = 0;
	protected long rowField = 0;
	protected long startTime = System.currentTimeMillis() / 1000;
	long max = 0L;

	public TableRowContentInspectJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
	}

	@Override
	public void run() {
		Thread.currentThread().setName(name);
		logger.info(String.format("Start compare the content of rows in table %s.%s and table %s.%s, the taskId is %s",
				source.getName(), inspectTask.getSource().getTable(),
				target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()));

		final InspectResultStats stats = new InspectResultStats();

		stats.setStart(new Date());
		stats.setStatus(InspectStatus.RUNNING.getCode());
		stats.setProgress(0);
		stats.setTaskId(inspectTask.getTaskId());
		stats.setSource(inspectTask.getSource());
		stats.setTarget(inspectTask.getTarget());
		stats.getSource().setConnectionName(source.getName());
		stats.getTarget().setConnectionName(target.getName());

		int retry = 0;
		while (retry < 4) {
			try {

				compare(inspectTask, source, target, stats, (inspectResultStats, inspectDetails) -> progressUpdateCallback.progress(inspectTask, stats, inspectDetails));
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

		logger.info(String.format("Inspect completed for task %s", inspectTask.getTaskId()));

		progressUpdateCallback.progress(inspectTask, stats, null);
	}

	private void compare(InspectTask inspectTask, Connections source, Connections target, InspectResultStats stats, CompareProgress compareProgress) {

		CompareFunction<Map<String, Object>, String> compareFn = null;

		boolean fullMatch = inspectTask.isFullMatch();
		InspectLimit inspectLimit = inspectTask.getLimit();
		long uniqueFieldLimit = inspectLimit != null ? inspectLimit.getKeep() : 1000;
		long otherFieldLimit = inspectLimit != null ? inspectLimit.getFullMatchKeep() : 100;
		uniqueFieldLimit = uniqueFieldLimit == 0 ? 1000 : uniqueFieldLimit;
		otherFieldLimit = otherFieldLimit == 0 ? 100 : otherFieldLimit;

		// force equals uniqueFieldLimit
		otherFieldLimit = uniqueFieldLimit;

		if (fullMatch) {
			if (null == inspectTask.getSource().getColumns() || inspectTask.getSource().getColumns().isEmpty()
					|| null == inspectTask.getTarget().getColumns() || inspectTask.getTarget().getColumns().isEmpty()) {
				compareFn = new DefaultCompare();
			} else {
				compareFn = new DefaultCompare(inspectTask.getSource().getColumns(), inspectTask.getTarget().getColumns());
			}
		}

		if (inspectTask.getBatchSize() > 0) {
			batchSize = inspectTask.getBatchSize();
		}


		if (logger.isDebugEnabled()) {
			try {
				logger.debug(JSONUtil.obj2JsonPretty(source));
				logger.debug(JSONUtil.obj2JsonPretty(target));
			} catch (JsonProcessingException ignore) {
			}
		}

		List<String> sourceKeys = getSortColumns(inspectTask.getSource().getSortColumn());
		List<String> targetKeys = getSortColumns(inspectTask.getTarget().getSortColumn());
		sourceKeys = sourceKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
		targetKeys = targetKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
		try (DiffDetailCursor diffDetailCursor = new DiffDetailCursor(inspectResultParentId, clientMongoOperator, sourceKeys, targetKeys)) {
			List<InspectDetail> inspectDetails = new ArrayList<>();
			long sourceTotal = 0;
			long targetTotal = 0;
			while (diffDetailCursor.next() && !Thread.interrupted()) {
				try (
						BaseResult<Map<String, Object>> sourceCursor = queryForCursor(source, inspectTask.getSource(), sourceNode, fullMatch, sourceKeys, diffDetailCursor.getData());
						BaseResult<Map<String, Object>> targetCursor = queryForCursor(target, inspectTask.getTarget(), targetNode, fullMatch, targetKeys, diffDetailCursor.getData())
				) {
					sourceTotal += sourceCursor.getTotal();
					targetTotal += targetCursor.getTotal();

					logger.info(sourceTotal + " -> " + targetTotal);

					boolean moveSource = true,
							moveTarget = true,
							sourceHasNext = true,
							targetHasNext = true;

					Map<String, Object> sourceRecord = null;
					Map<String, Object> targetRecord = null;
					if (diffDetailCursor.diffCounts() > 0) {
						// 差异数据数量
						max = diffDetailCursor.diffCounts();
					} else {
						// 全量校验，源和目标取最大数
						max = Math.max(sourceTotal, targetTotal);
					}

					while (moveSource || moveTarget) {

						if (moveSource) {
							sourceHasNext = sourceCursor.hasNext();
						}
						if (moveTarget) {
							targetHasNext = targetCursor.hasNext();
						}

						if (!sourceHasNext && !targetHasNext) {
							break;
						}

						current++;

						if (current % 5000 == 0) {
							double progress = getProgress();
							if (current % 20000 == 0) {
								logger.info("Compared " + current + ", total " + max + ", completed " + Math.round(progress * 100) + "%");
							}
							// long sourceTotal, long targetTotal, double progress, int cycles, long both, long source_only, long target_only, long row_passed, long row_failed, int speed
							stats.setSource_total(sourceTotal);
							stats.setTarget_total(targetTotal);
							stats.setProgress(progress);
							stats.setCycles(current);
							stats.setBoth(both);
							stats.setSource_only(sourceOnly);
							stats.setTarget_only(targetOnly);
							stats.setRow_passed(rowPassed);
							stats.setRow_failed(rowField);
							stats.setSpeed(current / (System.currentTimeMillis() / 1000 - startTime + 1));

							compareProgress.update(stats, null);
						}

						if (moveSource) {
							if (sourceHasNext) {
								sourceRecord = sourceCursor.next();
							} else {
								sourceRecord = Collections.emptyMap();
							}
						}
						if (moveTarget) {
							if (targetHasNext) {
								targetRecord = targetCursor.next();
							} else {
								targetRecord = Collections.emptyMap();
							}
						}

						String sourceVal = sourceCursor.getSortValue(sourceRecord);
						String targetVal = targetCursor.getSortValue(targetRecord);
						int compare;
						if (null == sourceVal) {
							compare = (null == targetVal) ? 0 : 1;
						} else {
							compare = (null == targetVal) ? -1 : sourceVal.compareTo(targetVal);
						}

						if (fullMatch && compare == 0) {
							String res = compareRecord(current, sourceVal, targetVal, sourceRecord, targetRecord, compareFn);
							if (null == res) {
								rowPassed++;
							} else {
								rowField++;
								if (otherFieldLimit > 0) {
									otherFieldLimit--;
									InspectDetail detail = new InspectDetail();

									detail.setSource(diffRecordTypeConvert(sourceRecord, inspectTask.getSource().getColumns()));
									detail.setTarget(diffRecordTypeConvert(targetRecord, inspectTask.getTarget().getColumns()));
									detail.setType("otherFields");
									detail.setMessage(res);

									inspectDetails.add(detail);
								}
							}
						}

						String msg = null;
						if (compare == 0 || null != diffDetailCursor.getData()) {
							moveSource = true;
							moveTarget = true;
							both++;
						} else if (compare < 0) { // ASC
							moveSource = true;
							moveTarget = false;
							msg = "SOURCE " + sourceVal;
							sourceOnly++;
							if (uniqueFieldLimit > 0) {
								uniqueFieldLimit--;
								InspectDetail detail = new InspectDetail();
								detail.setSource(diffRecordTypeConvert(sourceRecord, inspectTask.getSource().getColumns()));
								detail.setType("uniqueField");

								inspectDetails.add(detail);
							}
						} else {
							moveSource = false;
							moveTarget = true;
							msg = "TARGET " + targetVal;
							if (InspectDifferenceMode.isAll(inspectTaskContext.getInspectDifferenceMode())) {
								targetOnly++;
								if (uniqueFieldLimit > 0) {
									uniqueFieldLimit--;
									InspectDetail detail = new InspectDetail();
									detail.setTarget(diffRecordTypeConvert(targetRecord, inspectTask.getTarget().getColumns()));
									detail.setType("uniqueField");
									inspectDetails.add(detail);
								}
							}
						}

						if (!sourceHasNext) {
							moveSource = false;
							if (targetHasNext) {
								msg = "TARGET " + targetVal;
								moveTarget = true;
							}
						}
						if (!targetHasNext) {
							moveTarget = false;
							if (sourceHasNext) {
								msg = "SOURCE " + sourceVal;
								moveSource = true;
							}
						}
						if (msg != null) {
							logger.debug(msg);
						}

						if (inspectDetails.size() > 50) {
							compareProgress.update(stats, inspectDetails);
							inspectDetails = new ArrayList<>();
						}
					} // end while data

					// long sourceTotal, long targetTotal, double progress, int cycles, long both, long source_only, long target_only, long row_passed, long row_failed, int speed
					stats.setSource_total(sourceTotal);
					stats.setTarget_total(targetTotal);
				}
			} // end while diffDetailCursor

			stats.setCycles(current);
			stats.setBoth(both);
			stats.setSource_only(sourceOnly);
			stats.setTarget_only(targetOnly);
			stats.setRow_passed(rowPassed);
			stats.setRow_failed(rowField);
			stats.setSpeed(current / (System.currentTimeMillis() / 1000 - startTime + 1));

			stats.setEnd(new Date());
			stats.setProgress(1);
			stats.setResult((stats.getSource_only() > 0 || stats.getTarget_only() > 0 || stats.getRow_failed() > 0) ? "failed" : "passed");
			stats.setStatus(InspectStatus.DONE.getCode());

			compareProgress.update(stats, inspectDetails);
			logger.info("compare " + current);
		} catch (Throwable e) {
			stats.setEnd(new Date());
			stats.setResult("failed");
			stats.setStatus(InspectStatus.ERROR.getCode());
			stats.setErrorMsg(e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			compareProgress.update(stats, null);
		}
	}

	private double getProgress() {
		return new BigDecimal(current)
				.divide(new BigDecimal(current > max ? current + 1 : max), 4, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	private String compareRecord(long index, String sourceId, String targetId, Map<String, Object> sourceRecord, Map<String, Object> targetRecord, CompareFunction<Map<String, Object>, String> compareFn) {
		if (compareFn == null) {
			return "Compare fn is null.";
		}
		try {
			String result = compareFn.apply(sourceRecord, targetRecord, sourceId, targetId);
			if (null != result) {
				if (logger.isDebugEnabled()) {
					logger.debug("Different record: \n [" + sourceId + "]: " + gson.toJson(sourceRecord) + "\n -> \n [" + targetId + "]: " + gson.toJson(targetRecord));
//        } else {
//          logger.info(sourceId + " -> " + targetId + " has different fields.");
				}
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException("Call compare function failed: " + e.getMessage(), e);
		}
	}

	private BaseResult<Map<String, Object>> queryForCursor(Connections connections, InspectDataSource inspectDataSource, ConnectorNode connectorNode, boolean fullMatch, List<String> dataKeys, List<List<Object>> diffKeyValues) {
		inspectDataSource.setDirection("DESC"); // force desc
		Set<String> columns = null;
		if (null != inspectDataSource.getColumns()) {
			columns = new LinkedHashSet<>(inspectDataSource.getColumns());
		}
		return new PdkResult(
				getSortColumns(inspectDataSource.getSortColumn()),
				connections,
				inspectDataSource.getTable(),
				columns,
				connectorNode,
				fullMatch,
				dataKeys,
				diffKeyValues
		);
	}


	@FunctionalInterface
	public interface CompareProgress {

		void update(InspectResultStats stats, List<InspectDetail> inspectDetails);

	}
}
