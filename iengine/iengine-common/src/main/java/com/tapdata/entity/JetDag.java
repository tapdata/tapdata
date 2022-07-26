package com.tapdata.entity;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-07-29 14:40
 **/
public class JetDag implements Serializable {

	private static final long serialVersionUID = -6279685445298195786L;

	private DAG dag;
	private Map<String, AbstractProcessor> processorMap;
	private Map<String, AbstractProcessor> typeConvertMap;


	public JetDag(DAG dag, Map<String, AbstractProcessor> processorMap) {
		this.dag = dag;
		this.processorMap = processorMap;
	}

	public JetDag(DAG dag, Map<String, AbstractProcessor> processorMap, Map<String, AbstractProcessor> typeConvertMap) {
		this.dag = dag;
		this.processorMap = processorMap;
		this.typeConvertMap = typeConvertMap;
	}

	public DAG getDag() {
		return dag;
	}

	public Map<String, AbstractProcessor> getProcessorMap() {
		return processorMap;
	}

	public Map<String, AbstractProcessor> getTypeConvertMap() {
		return typeConvertMap;
	}
}
