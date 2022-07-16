package com.tapdata.validator;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.mongo.ClientMongoOperator;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public interface ValidateDataSource {

  void initialize(Connections connections, List<Mapping> mappings) throws Exception;

  Map<String, Object> data(Map<String, Object> recordKey, String fromTable, String toTable);

  void releaseResource();
}
