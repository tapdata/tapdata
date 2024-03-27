package io.tapdata.utils;

import org.springframework.data.mongodb.core.query.Update;

import java.util.Collection;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/3/14 16:50 Create
 */
public interface IUpdateOperation {

	default IUpdateOperation getParent() {
		return null;
	}

	String getKey();

	default String getFullKey() {
		IUpdateOperation parent = getParent();
		if (null != parent) {
			return parent.getFullKey() + "." + getKey();
		}
		return getKey();
	}

	default void set(Update update, boolean isSet, Object value) {
		if (!isSet) return;
		update.set(getFullKey(), value);
	}

	default void set(Update update, Object value) {
		update.set(getFullKey(), value);
	}

	default void setIfNotNull(Update update, Object value) {
		if (value == null) return;
		update.set(getFullKey(), value);
	}

	default void setIfNotBlank(Update update, String value) {
		if (value == null) return;

		String v = value.trim();
		if (v.isEmpty()) return;

		update.set(getFullKey(), v);
	}

	default void setIfNotEmpty(Update update, Collection<?> value) {
		if (null == value || value.isEmpty()) return;
		update.set(getFullKey(), value);
	}

	default void set(Map<String, Object> update, Object value) {
		update.put(getFullKey(), value);
	}

	default void setIfNotNull(Map<String, Object> update, Object value) {
		if (value == null) return;
		update.put(getFullKey(), value);
	}

	default void setIfNotBlank(Map<String, Object> update, String value) {
		if (value == null) return;

		String v = value.trim();
		if (v.isEmpty()) return;

		update.put(getFullKey(), v);
	}

	default void setIfNotEmpty(Map<String, Object> update, Collection<?> value) {
		if (null == value || value.isEmpty()) return;
		update.put(getFullKey(), value);
	}

	default <T> T get(Map<String, Object> event) {
		return (T) event.get(getFullKey());
	}

	default <T> T getOrDefault(Map<String, Object> event, T def) {
		Object val = event.get(getFullKey());
		if (null == val) return def;
		return (T) val;
	}

	default String getString(Map<String, Object> event) {
		Object val = event.get(getFullKey());
		return String.valueOf(val);
	}

	default String getIfExist(Map<String, Object> event, String def) {
		if (event.containsKey(getFullKey())) {
			Object val = event.get(getFullKey());
			if (null == val) return null;
			return val.toString();
		}
		return def;
	}

	default boolean getIfExist(Map<String, Object> event, boolean def) {
		if (event.containsKey(getFullKey())) {
			Object val = event.get(getFullKey());
			if (null == val) return false;
			return Boolean.parseBoolean(val.toString());
		}
		return def;
	}

}
