package io.tapdata.inspect.cdc;

import com.tapdata.entity.Connections;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectStatus;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.ProgressUpdate;
import io.tapdata.inspect.cdc.exception.InspectCdcNonsupportException;
import io.tapdata.inspect.cdc.exception.InspectCdcRunProfilesException;
import io.tapdata.pdk.core.api.ConnectorNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 增量校验任务
 * <ol>
 *   <li>
 *     开启调度时，获取上一次执行结果：
 *     <ul>
 *       <li>存在：取上次 '增量开始时间' 后移一个 '窗口时长'</li>
 *       <li>不存在：'增量开始时间' 不变</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 上午11:29 Create
 */
public abstract class AbsInspectCdcJob implements Runnable {
	private Logger logger = LogManager.getLogger(AbsInspectCdcJob.class);
	private static final int RETRY_COUNTS = 3;
	private static final int RETRY_TIMES = 5;
	private static final int DETAIL_BATCH_SIZE = 50;
	protected static final String RESULT_TYPE = "cdcCountResult";
	protected static final String RESULT_FAIL = "failed";
	protected static final String RESULT_PASSED = "passed";

	protected String name;
	protected com.tapdata.entity.inspect.InspectTask inspectTask;
	protected Connections source;
	protected Connections target;
	private ProgressUpdate progressUpdateCallback;
	private ClientMongoOperator clientMongoOperator;
	private ConnectorNode sourceNode, targetNode;
	private InspectResultStats stats;
	private List<InspectDetail> details = null;

	public AbsInspectCdcJob(InspectTaskContext inspectTaskContext) {
		this.inspectTask = inspectTaskContext.getTask();
		this.name = inspectTaskContext.getName() + "." + inspectTask.getTaskId();
		this.source = inspectTaskContext.getSource();
		this.target = inspectTaskContext.getTarget();
		this.progressUpdateCallback = inspectTaskContext.getProgressUpdateCallback();
		this.clientMongoOperator = inspectTaskContext.getClientMongoOperator();
		this.sourceNode = inspectTaskContext.getSourceConnectorNode();
		this.targetNode = inspectTaskContext.getTargetConnectorNode();

		stats = new InspectResultStats();
		stats.setCdcRunProfiles(inspectTask.getCdcRunProfiles());
		stats.setStart(new Date());
		stats.setStatus(InspectStatus.RUNNING.getCode());
		stats.setResult(RESULT_FAIL);
		stats.setProgress(0);
		stats.setTaskId(inspectTask.getTaskId());
		stats.setSource(inspectTask.getSource());
		stats.setTarget(inspectTask.getTarget());
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setName(name);
			if (null == source || null == target) {
				stats.setEnd(new Date());
				stats.setResult(RESULT_FAIL);
				stats.setStatus(InspectStatus.ERROR.getCode());

				stats.setErrorMsg(((null == source) ? "Source" : "Target") + " is null");
				progressUpdateCallback.progress(inspectTask, stats, null);
				return;
			}

			logger.info(String.format("Start compare the count of CDC in table %s.%s and table %s.%s, the taskId is %s",
					source.getName(), inspectTask.getSource().getTable(),
					target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()));

			// 执行校验，并在异常时重试
			errorRetry("CDC inspect failed", this::retryProcess, this::errorProcess);
			logger.info(String.format("CDC inspect completed for task %s", inspectTask.getTaskId()));
		} catch (Exception e) {
			logger.error(String.format("CDC inspect compare the count of CDC in table %s.%s and table %s.%s, the taskId is %s",
					source.getName(), inspectTask.getSource().getTable(),
					target.getName(), inspectTask.getTarget().getTable(), inspectTask.getTaskId()), e);
		}
	}

	// 校验逻辑
	private Void retryProcess() throws Exception {
		try (
				IInspectCdcOperator sourceCdc = IInspectCdcOperator.build(clientMongoOperator, inspectTask.getTaskId(), source, inspectTask.getSource().getTable(), true);
				IInspectCdcOperator targetCdc = IInspectCdcOperator.build(clientMongoOperator, inspectTask.getTaskId(), target, inspectTask.getTarget().getTable(), false)
		) {
			if (process(stats, sourceCdc, targetCdc)) {
				stats.setResult(RESULT_PASSED);
				stats.setProgress(1);
			} else {
				stats.setResult(RESULT_FAIL);
			}
			stats.setEnd(new Date());
			stats.setStatus(InspectStatus.DONE.getCode());
			detailBatchSubmit(stats);
		}
		return null;
	}

	/**
	 * 错误处理逻辑
	 *
	 * @param e       异常
	 * @param isRetry 是否重试
	 * @return 是否重试
	 */
	private Boolean errorProcess(Exception e, Boolean isRetry) {
		// 错误时清理结果
		if (null != details) {
			details.clear();
			details = null;
		}

		stats.setEnd(new Date());
		stats.setResult(RESULT_FAIL);
		stats.setStatus(InspectStatus.ERROR.getCode());
		stats.setErrorMsg(e.getMessage() + "\n" + Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n")));
		progressUpdateCallback.progress(inspectTask, stats, null);
		return isRetry;
	}

	/**
	 * 执行校验逻辑
	 *
	 * @param stats     状态信息
	 * @param sourceCdc 源增量操作接口
	 * @param targetCdc 目标增量接口
	 * @throws Exception 异常
	 */
	protected abstract boolean process(InspectResultStats stats, IInspectCdcOperator sourceCdc, IInspectCdcOperator targetCdc) throws Exception;

	/**
	 * 批量校验详情 - 提交
	 *
	 * @param stats 状态信息
	 */
	private void detailBatchSubmit(InspectResultStats stats) throws Exception {
		details = (null != details && !details.isEmpty()) ? details : null;
		errorRetry("Save CDC inspect details failed", () -> {
			progressUpdateCallback.progress(inspectTask, stats, details);
			return true;
		}, null);
		if (null != details) {
			details.clear();
			details = null;
		}
	}

	/**
	 * 批量校验详情 - 添加
	 *
	 * @param stats         状态信息
	 * @param inspectDetail 校验详情
	 */
	protected void detailAdd(InspectResultStats stats, InspectDetail inspectDetail) throws Exception {
		if (null != inspectDetail) {
			if (null == details) details = new ArrayList<>();
			details.add(inspectDetail);
		}

		if (null == details || details.isEmpty()) {
			errorRetry("Save CDC inspect details failed", () -> {
				progressUpdateCallback.progress(inspectTask, stats, null);
				return true;
			}, null);
		} else {
			errorRetry("Save CDC inspect details failed", () -> {
				progressUpdateCallback.progress(inspectTask, stats, details);
				details.clear();
				return true;
			}, null);
		}

	}

	/**
	 * 批量校验详情 - 添加
	 *
	 * @param stats         状态信息
	 * @param inspectDetail 校验详情
	 */
	protected void detailBatchAdd(InspectResultStats stats, InspectDetail inspectDetail) throws Exception {
		if (null == details) details = new ArrayList<>();
		details.add(inspectDetail);

		// 分批提交
		if (details.size() >= DETAIL_BATCH_SIZE) {
			errorRetry("Save CDC inspect details failed", () -> {
				progressUpdateCallback.progress(inspectTask, stats, details);
				details.clear();
				return true;
			}, null);
		}
	}

	/**
	 * 重试
	 *
	 * @param errMsg 错误信息
	 * @param fn     业务处理
	 * @param exFn   异常处理，返回：true 进行重试， false 抛出异常
	 * @throws Exception 异常
	 */
	public <T> T errorRetry(String errMsg, Callable<T> fn, BiFunction<Exception, Boolean, Boolean> exFn) throws Exception {
		return errorRetry(RETRY_COUNTS, RETRY_TIMES, errMsg, fn, exFn);
	}

	/**
	 * 重试
	 *
	 * @param retryCounts 重试次数
	 * @param retryTimes  重试等待时间
	 * @param errMsg      错误信息
	 * @param fn          业务处理
	 * @param exFn        异常处理，返回：true 重试， false 抛出异常
	 * @throws Exception 异常
	 */
	public <T> T errorRetry(int retryCounts, int retryTimes, String errMsg, Callable<T> fn, BiFunction<Exception, Boolean, Boolean> exFn) throws Exception {
		for (int retries = 1; true; retries++) {
			try {
				return fn.call();
			} catch (Exception e) {
				boolean isRetry = !(retries > retryCounts || e instanceof InspectCdcNonsupportException || e instanceof InspectCdcRunProfilesException);
				if (null != exFn && !exFn.apply(e, isRetry) || !isRetry) throw e;
				logger.warn(String.format("%s(%s)", errMsg, retries), e);
				if (retryTimes > 0) TimeUnit.SECONDS.sleep(retryTimes);
			}
		}
	}
}
