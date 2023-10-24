package io.tapdata.pdk.apis.entity.merge;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-05-27 16:18
 **/
public class MergeLookupResult implements Serializable {
	private static final long serialVersionUID = -6100854075182627105L;
	private MergeTableProperties property;
	private Map<String, Object> data;

	private List<MergeLookupResult> mergeLookupResults;

	public MergeTableProperties getProperty() {
		return property;
	}

	public void setProperty(MergeTableProperties property) {
		this.property = property;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public List<MergeLookupResult> getMergeLookupResults() {
		return mergeLookupResults;
	}

	public void setMergeLookupResults(List<MergeLookupResult> mergeLookupResults) {
		this.mergeLookupResults = mergeLookupResults;
	}
}
