package com.tapdata.constant;

import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;

import java.util.Date;

/**
 * Created by xj
 * 2020-03-19 10:18
 **/
public class CronUtil {

	/**
	 * 获取上次执行时间
	 *
	 * @param cron
	 * @return
	 */
	public static long getLastTriggerTime(String cron) {
		if (!CronExpression.isValidExpression(cron)) {
			return 0;
		}
		CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date")
				.withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
		Date time0 = trigger.getStartTime();
		Date time1 = trigger.getFireTimeAfter(time0);
		Date time2 = trigger.getFireTimeAfter(time1);
		Date time3 = trigger.getFireTimeAfter(time2);
		long l = time1.getTime() - (time3.getTime() - time2.getTime());
		return l;
	}

	/**
	 * 获取下次执行时间
	 *
	 * @param cron
	 * @return
	 */
	public static long getNextTriggerTime(String cron) {
		if (cron == null || cron.length() == 0 || !CronExpression.isValidExpression(cron)) {
			return 0;
		}
		CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date")
				.withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
		Date time0 = trigger.getStartTime();
		Date time1 = trigger.getFireTimeAfter(time0);
		return time1.getTime();
	}

	public static void main(String[] args) {
		String cronString = "0 0 1 * * ?";
		Date date = new Date(CronUtil.getNextTriggerTime(cronString));
		System.out.println(date.toString());
	}
}

