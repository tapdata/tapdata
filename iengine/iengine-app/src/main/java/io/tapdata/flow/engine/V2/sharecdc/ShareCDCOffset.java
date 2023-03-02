package io.tapdata.flow.engine.V2.sharecdc;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-02-27 21:32
 **/
public class ShareCDCOffset {
	private Map<String, Long> sequenceMap;
	private Object streamOffset;

	public ShareCDCOffset(Map<String, Long> sequenceMap, Object streamOffset) {
		this.sequenceMap = sequenceMap;
		this.streamOffset = streamOffset;
	}

	public Map<String, Long> getSequenceMap() {
		return sequenceMap;
	}

	public Object getStreamOffset() {
		return streamOffset;
	}
}
