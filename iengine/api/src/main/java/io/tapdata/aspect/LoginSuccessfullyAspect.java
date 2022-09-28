package io.tapdata.aspect;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.User;
import io.tapdata.entity.aspect.Aspect;

import java.util.List;

public class LoginSuccessfullyAspect extends Aspect {
	private List<String> baseUrls;
	public LoginSuccessfullyAspect baseUrls(List<String> baseUrls) {
		this.baseUrls = baseUrls;
		return this;
	}
	private ConfigurationCenter configCenter;
	public LoginSuccessfullyAspect configCenter(ConfigurationCenter configCenter) {
		this.configCenter = configCenter;
		return this;
	}
	private User user;
	public LoginSuccessfullyAspect user(User user) {
		this.user = user;
		return this;
	}

	public ConfigurationCenter getConfigCenter() {
		return configCenter;
	}

	public void setConfigCenter(ConfigurationCenter configCenter) {
		this.configCenter = configCenter;
	}

	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public List<String> getBaseUrls() {
		return baseUrls;
	}

	public void setBaseUrls(List<String> baseUrls) {
		this.baseUrls = baseUrls;
	}
}
