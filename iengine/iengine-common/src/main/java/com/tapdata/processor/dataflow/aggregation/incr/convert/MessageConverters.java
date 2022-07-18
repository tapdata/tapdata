package com.tapdata.processor.dataflow.aggregation.incr.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MessageConverters {

	private static final Map<String, MessageConverter> MESSAGE_CONVERTER_MAP = new HashMap<>();

	public static void register(MessageConverter converter) {
		MESSAGE_CONVERTER_MAP.put(converter.getOp().getType(), converter);
	}

	public static MessageConverter ofOperation(String op) {
		return Optional.ofNullable(MESSAGE_CONVERTER_MAP.get(op)).orElseThrow(() -> new IllegalArgumentException(String.format("unsupported message operation: %s", op)));
	}


}
