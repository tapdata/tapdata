package io.tapdata.metric;

import io.tapdata.common.SettingService;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

public class SettingWrapper {

	private final SettingService settingService;

	public SettingWrapper(SettingService settingService) {
		this.settingService = settingService;
	}

	public double getDouble(String prop, double defaultValue) {
		return this.getString(prop).map(Double::parseDouble).orElse(defaultValue);
	}

	public long getLong(String prop, long defaultValue) {
		return this.getString(prop).map(Long::parseLong).orElse(defaultValue);
	}

	private Optional<String> getString(String prop) {
		return Optional.ofNullable(settingService.getString(prop)).filter(StringUtils::isNotEmpty);
	}

}
