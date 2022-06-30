package com.tapdata.tm.commons.base.convert;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;

import java.io.IOException;


/**
 * @Author: Zed
 * @Date: 2021/11/6
 * @Description:
 */
public class DagDeserialize extends JsonDeserializer<DAG> {
	@Override
	public DAG deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		Dag dag = p.getCodec().readValue(p, Dag.class);
		return DAG.build(dag);
	}
}
