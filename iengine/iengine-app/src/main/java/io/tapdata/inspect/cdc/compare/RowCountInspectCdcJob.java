package io.tapdata.inspect.cdc.compare;

import com.tapdata.entity.inspect.InspectCdcRunProfiles;
import com.tapdata.entity.inspect.InspectCdcWinData;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.entity.inspect.InspectResultStats;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.inspect.cdc.AbsInspectCdcJob;
import io.tapdata.inspect.cdc.IInspectCdcOperator;
import io.tapdata.inspect.cdc.InspectCdcUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

/**
 * 增量统计校验
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 上午10:53 Create
 */
public class RowCountInspectCdcJob extends AbsInspectCdcJob {
	private final static Logger logger = LogManager.getLogger(RowCountInspectCdcJob.class);
	private final static int MIN_WIN_DURATION = 30; // 最小窗口时长

	public RowCountInspectCdcJob(InspectTaskContext inspectTaskContext) {
		super(inspectTaskContext);
	}

	@Override
	protected boolean process(InspectResultStats stats, IInspectCdcOperator sourceCdc, IInspectCdcOperator targetCdc) throws Exception {
		InspectCdcRunProfiles runProfiles = stats.getCdcRunProfiles();
		InspectCdcUtils.validate(runProfiles);

		int winDuration = runProfiles.getWinDuration();
		Instant winBegin = runProfiles.getCdcBeginDate();

		Instant lastEventDate = sourceCdc.lastEventDate();
		if (null == lastEventDate) {
			stats.setErrorMsg("Not found cdc event");
			logger.warn(stats.getErrorMsg());
			return true;
		} else if (winBegin.isAfter(lastEventDate)) {
			// 开始时间 > 最后一条事件时间，直接返回
			stats.setErrorMsg("Begin date after last event date");
			logger.warn(stats.getErrorMsg());
			return true;
		}

		Instant winEnd;
		{
			// 推进截止时间
			Instant endDate = runProfiles.getCdcEndDate();
			winEnd = InspectCdcUtils.addByDuration(winBegin, winDuration);
			if (null == endDate) {
				// 结束时间为空：推进到最后一条事件的窗口
				while (winEnd.isBefore(lastEventDate)) {
					winEnd = InspectCdcUtils.addByDuration(winEnd, winDuration);
				}
			} else {
				// 有结束时间：推进到结束时间窗口 || 最后一条事件窗口
				while (true) {
					if (endDate.isAfter(winEnd)) {
						winEnd = endDate;
						break;
					} else if (winEnd.isAfter(lastEventDate)) {
						break;
					}
					winEnd = InspectCdcUtils.addByDuration(winEnd, winDuration);
				}
			}
		}

		String sourceOffset = runProfiles.getSourceOffset();
		String targetOffset = runProfiles.getTargetOffset();
		InspectCdcWinData sourceCdcWinData;
		InspectCdcWinData targetCdcWinData;

		// 分批统计结果
		long sourceCounts, targetCounts;
		Instant batchBegin, batchEnd;
		int batchDuration = (int) Math.ceil((winEnd.getEpochSecond() - winBegin.getEpochSecond()) / 60.0), batchSize = 1;
		// 默认不开启自动拆分，看上去没有性能问题，如果有再打开这个代码
		boolean isUseInnerSplit = false;
		if (isUseInnerSplit) {
			batchDuration = MIN_WIN_DURATION;
			batchSize = InspectCdcUtils.getBatchSize(winBegin, winEnd, batchDuration);
		}
		logger.info("Inspect CDC counts between '{}' and '{}', last event date {}, batch size {} duration {}, begin offset {}", winBegin, winEnd, lastEventDate, batchSize, batchDuration, targetOffset);
		for (int i = 0; i < batchSize; i++) {
			batchBegin = InspectCdcUtils.addByDuration(winBegin, i * batchDuration);
			batchEnd = InspectCdcUtils.addByDuration(batchBegin, batchDuration);

			// 统计
			sourceCdcWinData = new InspectCdcWinData(batchBegin, batchEnd, sourceOffset);
			sourceCounts = sourceCdc.count(sourceCdcWinData);
			targetCdcWinData = new InspectCdcWinData(batchBegin, batchEnd, targetOffset);
			targetCounts = targetCdc.count(targetCdcWinData);

			// 更新运行配置
			sourceOffset = sourceCdcWinData.getEndOffset();
			targetOffset = targetCdcWinData.getEndOffset();
			// 更新状态
			stats.setSource_total(stats.getSource_total() + sourceCounts);
			stats.setTarget_total(stats.getTarget_total() + targetCounts);
			stats.setSource_only(stats.getSource_only() + sourceCounts);
			stats.setTarget_only(stats.getTarget_only() + targetCounts);
			stats.setResult(sourceCounts == targetCounts ? RESULT_PASSED : RESULT_FAIL);
			stats.setProgress(1.0 * i / batchSize);

			// 因为一次处理多个窗口数据，所以需要阶段性保存信息
			if (InspectCdcUtils.needSave(winBegin, batchBegin, winDuration) && lastEventDate.isAfter(batchEnd)) {
				// 有更多数据推进时间窗口
				runProfiles.setSourceOffset(sourceOffset);
				runProfiles.setTargetOffset(targetOffset);
				runProfiles.setCdcBeginDate(batchEnd);
				stats.setCdcRunProfiles(runProfiles);
				logger.info("Inspect CDC need to save run profiles: {}", runProfiles);
			}

			// 只输出错误结果
			if (RESULT_FAIL.equals(stats.getResult())) {
				InspectDetail detail = new InspectDetail();
				detail.setType(RESULT_TYPE);
				detail.setSource(sourceCdc.cdcWinData2Map(sourceCdcWinData, sourceCounts));
				detail.setTarget(targetCdc.cdcWinData2Map(targetCdcWinData, targetCounts));
				detailAdd(stats, detail);
			} else {
				detailAdd(stats, null);
			}
		}

		if (stats.getSource_total() != stats.getTarget_total()) {
			stats.setErrorMsg(String.format("Source counts %s, Target counts: %s", stats.getSource_total(), stats.getTarget()));
			return false;
		}

		// 有更多数据推进时间窗口
		if (lastEventDate.isAfter(winEnd)) {
			runProfiles.setSourceOffset(sourceOffset);
			runProfiles.setTargetOffset(targetOffset);
			runProfiles.setCdcBeginDate(winEnd);
		}
		stats.setCdcRunProfiles(runProfiles);
		return true;
	}
}
