package io.tapdata.common.sharecdc;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.entity.sharecdc.ShareCdcConstant;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.SettingService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author samuel
 * @Description
 * @create 2022-01-27 10:35
 **/
public class ShareCdcUtil {

	private final static Logger logger = LogManager.getLogger(ShareCdcUtil.class);
	private final static String SHARE_CDC_KEY_PREFIX = "SHARE_CDC_";

	public static String getConstructName(TaskDto taskDto) {
		return SHARE_CDC_KEY_PREFIX + taskDto.getName();
	}

	public static boolean shareCdcEnable(SettingService settingService) {
		assert settingService != null;
		settingService.loadSettings(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE);
		String shareCdcEnable = settingService.getString(ShareCdcConstant.SETTING_SHARE_CDC_ENABLE, "true");
		try {
			return Boolean.parseBoolean(shareCdcEnable);
		} catch (Exception e) {
			logger.warn("Get global share cdc enable setting failed, key: " + ShareCdcConstant.SETTING_SHARE_CDC_ENABLE
					+ ", will use default value: true"
					+ "; Error: " + e.getMessage() + "\n" + Log4jUtil.getStackString(e));
			return true;
		}
	}
}
