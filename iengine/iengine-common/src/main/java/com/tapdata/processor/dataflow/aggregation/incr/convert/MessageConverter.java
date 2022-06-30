package com.tapdata.processor.dataflow.aggregation.incr.convert;

import com.tapdata.entity.MessageEntity;

import java.util.Collection;

public interface MessageConverter {

	Collection<MessageEntity> convert(MessageEntity originMessage);

	MessageOp getOp();

}
