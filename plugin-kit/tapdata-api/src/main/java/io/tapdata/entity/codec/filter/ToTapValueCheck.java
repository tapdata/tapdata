package io.tapdata.entity.codec.filter;

import io.tapdata.entity.codec.detector.TapDetector;

/**
 * @author aplomb
 */
public interface ToTapValueCheck extends TapDetector {
	boolean check(String key, Object value);
}
