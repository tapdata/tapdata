package com.tapdata.processor;

import com.tapdata.entity.MessageEntity;

import java.util.List;

/**
 * @author jackin
 */
public interface Processor {

	List<MessageEntity> process(List<MessageEntity> batch);

	void stop();
}
