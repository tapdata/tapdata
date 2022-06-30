package io.tapdata.pdk.apis.entity.merge;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-05-27 17:02
 **/
public class MergeInfo {
	public static final String EVENT_INFO_KEY = "MERGE_INFO";
	private MergeTableProperties currentProperty;
	private List<MergeLookupResult> mergeLookupResults;

	public MergeTableProperties getCurrentProperty() {
		return currentProperty;
	}

	public void setCurrentProperty(MergeTableProperties currentProperty) {
		this.currentProperty = currentProperty;
	}

	public List<MergeLookupResult> getMergeLookupResults() {
		return mergeLookupResults;
	}

	public void setMergeLookupResults(List<MergeLookupResult> mergeLookupResults) {
		this.mergeLookupResults = mergeLookupResults;
	}

	public MergeInfo addLookupResult(MergeLookupResult mergeLookupResult) {
		if (null == mergeLookupResults) mergeLookupResults = new ArrayList<>();
		this.mergeLookupResults.add(mergeLookupResult);
		return this;
	}
}
