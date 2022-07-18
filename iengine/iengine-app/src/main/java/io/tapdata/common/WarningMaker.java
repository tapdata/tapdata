package io.tapdata.common;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Event;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WarningMaker {

	private Set<String> triggers;

	private ClientMongoOperator clientMongoOperator;

	public WarningMaker(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public WarningMaker(List<String> triggers) {
		if (CollectionUtils.isNotEmpty(triggers)) {
			this.triggers = new HashSet<>(triggers.size());
			for (String trigger : triggers) {
				this.triggers.add(trigger);
			}
		}
	}

	public Event generateEvent(Event.EventName eventName, Map<String, Object> event_data, String tag, String jobId) {
		Event event = new Event();
		event.setName(eventName.name);
		event.setEvent_data(event_data);
		event.setTag(tag);
		event.setJob_id(jobId);

		clientMongoOperator.insertOne(event, ConnectorConstant.EVENT_COLLECTION);
		return event;
	}

	public boolean match(String event) {
		if (triggers != null) {
			return triggers.contains(event);
		}
		return false;
	}

	public void setTriggers(Set<String> triggers) {
		this.triggers = triggers;
	}

	public Set<String> getTriggers() {
		return triggers;
	}

}
