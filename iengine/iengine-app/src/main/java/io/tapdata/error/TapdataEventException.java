package io.tapdata.error;

import com.tapdata.entity.TapdataEvent;
import io.tapdata.exception.TapCodeException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-04-14 17:41
 **/
public class TapdataEventException extends TapCodeException {
	private static final long serialVersionUID = -6189587384294871969L;
	private List<TapdataEvent> events = new ArrayList<>();

	public TapdataEventException(String code) {
		super(code);
	}

	public TapdataEventException(String code, String message) {
		super(code, message);
	}

	public TapdataEventException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public TapdataEventException(String code, Throwable cause) {
		super(code, cause);
	}
	public TapdataEventException events(List<TapdataEvent> events) {
		this.events = events;
		return this;
	}

	public TapdataEventException addEvent(TapdataEvent event) {
		if (null == event) {
			return this;
		}
		this.events.add(event);
		return this;
	}

	public List<TapdataEvent> getEvents() {
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
