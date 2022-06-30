package io.tapdata.common;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Setting;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by tapdata on 13/03/2018.
 */
public class SettingService {

	private static final Logger logger = LogManager.getLogger(SettingService.class);
	private static final String LOG_PREFIX = "[Setting] - ";

	private final Map<String, Setting> settingMap = new ConcurrentHashMap<>();

	private ClientMongoOperator clientMongoOperator;
	private AtomicBoolean logged = new AtomicBoolean(true);

	public SettingService() {
	}

	public SettingService(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public void loadSettings() {
		if (logged.get()) {
			logger.info(LOG_PREFIX + "Loading tapdata settings...\n" + Arrays.toString(Thread.currentThread().getStackTrace()));
		}
		List<Setting> settings = clientMongoOperator.find(new Query(), ConnectorConstant.SETTING_COLLECTION + "?decode=1", Setting.class);
		for (Setting setting : settings) {
			settingMap.put(setting.getKey(), setting);
			if (logged.get() && logger.isDebugEnabled()) {
				logger.debug(LOG_PREFIX + "Load setting: " + setting);
			}
		}
		logged.set(false);
	}

	public void loadSettings(String key) {
		Query query = new Query(Criteria.where("key").is(key));
		Setting setting = clientMongoOperator.findOne(query, ConnectorConstant.SETTING_COLLECTION + "?decode=1", Setting.class);
		settingMap.put(setting.getKey(), setting);
		logger.info(LOG_PREFIX + "Load setting: " + setting);
	}

	public Setting getSetting(String key) {
		return settingMap.get(key);
	}

	public String getString(String key) {
		return getString(key, "");
	}

	public String getString(String key, String defaultValue) {
		String i = defaultValue;
		if (StringUtils.isNotBlank(key)) {
			Setting setting = this.getSetting(key);
			if (setting != null) {
				String value = setting.getValue();
				if (StringUtils.isNotBlank(value)) {
					i = value;
				} else {
					String default_value = setting.getDefault_value();
					if (StringUtils.isNotBlank(default_value)) {
						i = default_value;
					}
				}
			}
		}

		return i;
	}

	public int getInt(String key, int defaultValue) {
		int i = defaultValue;
		if (StringUtils.isNotBlank(key)) {
			Setting setting = this.getSetting(key);
			if (setting != null) {
				String value = setting.getValue();
				if (StringUtils.isNotBlank(value)) {
					try {
						i = Integer.parseInt(value);
					} catch (NumberFormatException e) {
						// do nothing
					}
				} else {
					String default_value = setting.getDefault_value();
					if (StringUtils.isNotBlank(default_value)) {
						try {
							i = Integer.parseInt(default_value);
						} catch (NumberFormatException e) {
							// do nothing
						}
					}
				}
			}
		}

		return i;
	}

	public long getLong(String key, long defaultValue) {
		long i = defaultValue;
		if (StringUtils.isNotBlank(key)) {
			Setting setting = this.getSetting(key);
			if (setting != null) {
				String value = setting.getValue();
				if (StringUtils.isNotBlank(value)) {
					try {
						i = Long.parseLong(value);
					} catch (NumberFormatException e) {
						// do nothing
					}
				} else {
					String default_value = setting.getDefault_value();
					if (StringUtils.isNotBlank(default_value)) {
						try {
							i = Long.parseLong(default_value);
						} catch (NumberFormatException e) {
							// do nothing
						}
					}
				}
			}
		}

		return i;
	}

	public Map<String, Setting> getSettingMap() {
		return settingMap;
	}

	public void setValue(String key, String value) {
		Setting setting = this.settingMap.get(key);
		if (null == setting) return;
		setting.setValue(value);
	}
}
