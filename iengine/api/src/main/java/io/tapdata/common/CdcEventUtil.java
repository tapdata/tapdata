package io.tapdata.common;

/**
 * @author samuel
 * @Description
 * @create 2020-09-07 12:02
 **/
public class CdcEventUtil {
	public static boolean needSaveCdcEvent(SettingService settingService) {
		if (settingService == null) {
			return false;
		}

		String jobCdcRecord = settingService.getString("job_cdc_record");
		return jobCdcRecord.equals("true");
	}
}
