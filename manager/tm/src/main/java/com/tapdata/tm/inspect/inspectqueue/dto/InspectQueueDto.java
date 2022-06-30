/**
 * @title: MessageQueueDto
 * @description:
 * @author lk
 * @date 2021/9/7
 */
package com.tapdata.tm.inspect.inspectqueue.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.ws.dto.GranularityInfo;

import java.util.Map;

public class InspectQueueDto extends BaseDto {

	private String type;

	private Map<String, Object> data; // testConnection

	private String sender;

	private String receiver;

	private Map<String, Object> filter; // logs、watch

	private String collection; // watch

	private String dataFlowId;  // dataFlowInsight

	private GranularityInfo granularity; // dataFlowInsight

	private String messageType; // unsubscribe

	public String getType() {
		return type;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public String getSender() {
		return sender;
	}

	public String getReceiver() {
		return receiver;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	public Map<String, Object> getFilter() {
		return filter;
	}

	public void setFilter(Map<String, Object> filter) {
		this.filter = filter;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public GranularityInfo getGranularity() {
		return granularity;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public void setGranularity(GranularityInfo granularity) {
		this.granularity = granularity;
	}

	public String getMessageType() {
		return messageType;
	}

	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}
}
