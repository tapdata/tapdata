package io.tapdata.inspect.cdc;

import com.tapdata.entity.inspect.Inspect;
import com.tapdata.entity.inspect.InspectCdcRunProfiles;
import com.tapdata.entity.inspect.InspectMethod;
import com.tapdata.entity.inspect.InspectResult;
import com.tapdata.entity.inspect.InspectResultStats;
import com.tapdata.entity.inspect.InspectTask;
import io.tapdata.inspect.InspectService;
import io.tapdata.inspect.cdc.exception.InspectCdcConfException;
import io.tapdata.inspect.cdc.exception.InspectCdcRunProfilesException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.SimpleTimeZone;

/**
 * 增量校验工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/4 下午8:22 Create
 */
public interface InspectCdcUtils {

	// 时间配置格式
	String DATE_FORMAT = "yyyy-MM-dd HH:mm";

	/**
	 * 解析时间
	 *
	 * @param dateStr        时间字符串
	 * @param timezoneOffset 时区偏移量，分钟
	 * @return 时间
	 * @throws ParseException 异常
	 */
	static Instant parseDate(String dateStr, int timezoneOffset) throws ParseException {
		if (null == dateStr || dateStr.isEmpty()) return null;
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
		sdf.setTimeZone(new SimpleTimeZone(0, "XSD 'Z' timezone"));
		Instant instant = sdf.parse(dateStr).toInstant();
		return instant.plus(timezoneOffset, ChronoUnit.MINUTES);
	}

	/**
	 * 格式化时间
	 *
	 * @param date 时间
	 * @return 字符串
	 */
	static String format(Instant date) {
		if (null == date) return null;
		return date.toString();
	}

	/**
	 * 时间添加窗口大小
	 *
	 * @param date     时间
	 * @param duration 窗口大小，单位：分钟
	 * @return 新时间
	 */
	static Instant addByDuration(Instant date, int duration) {
		return date.plus(duration, ChronoUnit.MINUTES);
	}

	/**
	 * 计算批次
	 *
	 * @param winBegin      窗口开始时间
	 * @param winEnd        窗口结束时间
	 * @param batchDuration 批次窗口时长
	 * @return 执行批次
	 */
	static int getBatchSize(Instant winBegin, Instant winEnd, int batchDuration) {
		double allDuration = (double) (winEnd.getEpochSecond() - winBegin.getEpochSecond()) / 60;
		return (int) Math.ceil(allDuration / batchDuration);
	}

	/**
	 * 判断当前窗口是否需要保存偏移量信息
	 *
	 * @param winBegin    窗口开始时间
	 * @param batchBegin  批处理开始时间
	 * @param winDuration 窗口时长
	 * @return 是否需要保存
	 */
	static boolean needSave(Instant winBegin, Instant batchBegin, int winDuration) {
		long times = (batchBegin.getEpochSecond() - winBegin.getEpochSecond()) / 60;
		return times % winDuration == 0;
	}

	/**
	 * 是否增量校验任务
	 *
	 * @param inspect 校验任务信息
	 * @return 是否增量校验任务
	 */
	static boolean isInspectCdc(Inspect inspect) {
		return InspectMethod.CDC_COUNT.equalsString(inspect.getInspectMethod());
	}

	/**
	 * 设置增量运行配置
	 *
	 * @param inspectService 校验服务实例
	 * @param inspectResult  校验结果
	 */
	static void setCdcRunProfilesByLastResult(InspectService inspectService, InspectResult inspectResult) {
		// 如果是调度模式 && 版本号一致，需要填充上次运行配置
		Inspect inspect = inspectResult.getInspect();
		if (null == inspect || !Inspect.Mode.CRON.getValue().equals(inspect.getMode())) return;
		if (null == inspect.getVersion()) {
			if (null != inspectResult.getInspectVersion()) return;
		} else if (!inspect.getVersion().equals(inspectResult.getInspectVersion())) {
			return;
		}

		InspectResult lastResult = inspectService.getLastInspectResult(inspectResult.getInspect_id());
		if (null != lastResult && null != lastResult.getStats() && null != inspect.getTasks()) {
			for (InspectResultStats stats : lastResult.getStats()) {
				for (InspectTask task : inspectResult.getInspect().getTasks()) {
					if (stats.getTaskId().equals(task.getTaskId())) {
						task.setCdcRunProfiles(stats.getCdcRunProfiles());
					}
				}
			}
		}
	}

	/**
	 * 初始化增量运行配置
	 *
	 * @param inspect 校验信息
	 * @param task    校验任务
	 */
	static void initCdcRunProfiles(Inspect inspect, InspectTask task) {
		// 设置窗口时间
		int duration = inspect.getCdcDuration();
		duration = Math.max(5, duration);
		inspect.setCdcDuration(duration);

		// 开始时间
		Instant beginDate = null;
		try {
			String tmpStr = inspect.getCdcBeginDate();
			beginDate = parseDate(tmpStr, inspect.getBrowserTimezoneOffset());
		} catch (ParseException e) {
			InspectCdcConfException.throwIfTrue(true, "Begin date is invalid");
		}
		InspectCdcConfException.throwIfTrue(null == beginDate, "Begin date can not be empty");

		// 如果为空，初始化一个
		if (null == task.getCdcRunProfiles()) {
			task.setCdcRunProfiles(new InspectCdcRunProfiles(beginDate, duration));
		}
		// 调度启动时，结束时间无效
		if (Inspect.Mode.CRON.getValue().equals(inspect.getMode())) {
			task.getCdcRunProfiles().setCdcEndDate(null);
		}
	}

	/**
	 * 检查配置是否合法
	 *
	 * @param runProfiles 运行配置
	 */
	static void validate(InspectCdcRunProfiles runProfiles) {
		if (null == runProfiles) {
			throw new InspectCdcRunProfilesException("The run profiles is null");
		} else if (null == runProfiles.getCdcBeginDate()) {
			throw new InspectCdcRunProfilesException("The 'cdcBegin' is null, run profiles: " + runProfiles);
		} else if (runProfiles.getWinDuration() <= 0) {
			throw new InspectCdcRunProfilesException("The 'winDuration' is invalid, run profiles: " + runProfiles);
		}
	}
}
