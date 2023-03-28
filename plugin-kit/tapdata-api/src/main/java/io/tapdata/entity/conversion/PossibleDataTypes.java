package io.tapdata.entity.conversion;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author aplomb
 */
public class PossibleDataTypes implements Serializable {
	private List<String> dataTypes;
	public PossibleDataTypes dataType(String dataType) {
		if(dataTypes == null)
			dataTypes = new ArrayList<>();
		dataTypes.add(dataType);
		return this;
	}
	private String lastMatchedDataType;
	public PossibleDataTypes lastMatchedDataType(String lastMatchedDataType) {
		this.lastMatchedDataType = lastMatchedDataType;
		return this;
	}

	public List<String> getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(List<String> dataTypes) {
		this.dataTypes = dataTypes;
	}

	public String getLastMatchedDataType() {
		return lastMatchedDataType;
	}

	public void setLastMatchedDataType(String lastMatchedDataType) {
		this.lastMatchedDataType = lastMatchedDataType;
	}
}
