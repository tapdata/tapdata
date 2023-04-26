package io.tapdata.flow.engine.V2.node.hazelcast.processor.aggregation;

import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.TapdataEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * @Author dayun
 * @Date 7/19/22
 * 处理器Aggregator之间传递的消息体定义
 */
@Setter
@Getter
public class WrapItem implements Cloneable {
	private Object message;
	/**
	 * 事件造成行数的变化
	 */
	private BigDecimal changedCount;

	private Object originalMessage;

	private String cachedGroupByKey;

	private BigDecimal cachedRollingAggregateCounter;

	/***
	 * 把输入参数带着输出
	 * FinishP中带出去
	 */
	private TapdataEvent event;

	@Override
	public WrapItem clone() {
		WrapItem newItem = new WrapItem();
		newItem.setMessage(message);
		newItem.setOriginalMessage(originalMessage);
		newItem.setChangedCount(changedCount);
		newItem.setCachedGroupByKey(cachedGroupByKey);
		newItem.setCachedRollingAggregateCounter(cachedRollingAggregateCounter);
		newItem.setEvent((TapdataEvent) event.clone());
		return newItem;
	}

	public boolean isEvent() {
		return this.message instanceof MessageEntity || this.message instanceof TapRecordEvent;
	}

	public boolean isNotEvent() {
		return !isEvent();
	}

	public boolean isMessageEntity() {
		return this.message instanceof MessageEntity;
	}

	public boolean isTapRecordEvent() {
		return this.message instanceof TapRecordEvent;
	}
}
