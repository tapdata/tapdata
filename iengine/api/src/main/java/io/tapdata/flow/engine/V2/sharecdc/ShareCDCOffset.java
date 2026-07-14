package io.tapdata.flow.engine.V2.sharecdc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-02-27 21:32
 **/
public class ShareCDCOffset implements Serializable {

	private static final long serialVersionUID = 4290623022849235468L;
	private Map<String, Long> sequenceMap;
	private Object streamOffset;

	public ShareCDCOffset(Map<String, Long> sequenceMap, Object streamOffset) {
		this.sequenceMap = null == sequenceMap ? null : new HashMap<>(sequenceMap);
		this.streamOffset = streamOffset;
	}

	public Map<String, Long> getSequenceMap() {
		return null == sequenceMap ? null : new HashMap<>(sequenceMap);
	}

	public Object getStreamOffset() {
		return streamOffset;
	}
}
