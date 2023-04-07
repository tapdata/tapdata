package io.tapdata.error;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.exception.TapCodeException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-04-07 18:42
 **/
public class TapEventException extends TapCodeException {
	private static final long serialVersionUID = 4417232567590332999L;
	private List<TapEvent> events = new ArrayList<>();

	public TapEventException(String code) {
		super(code);
	}

	public TapEventException(String code, String message) {
		super(code, message);
	}

	public TapEventException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public TapEventException(String code, Throwable cause) {
		super(code, cause);
	}

	public TapEventException events(List<TapEvent> events) {
		this.events = events;
		return this;
	}

	public TapEventException addEvent(TapEvent event) {
		if (null == event) {
			return this;
		}
		this.events.add(event);
		return this;
	}

	public List<TapEvent> getEvents() {
		return events;
	}

	@Override
	public String getMessage() {
		StringBuilder stringBuilder = new StringBuilder(super.getMessage());
		if (CollectionUtils.isNotEmpty(events)) {
			events.forEach(event -> stringBuilder.append("\n - ").append(event.toString()));
		}
		return stringBuilder.toString();
	}
}
