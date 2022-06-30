package io.tapdata.debug.impl;

import com.tapdata.constant.MapUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.dataflow.Stage;
import io.tapdata.debug.DebugConstant;
import io.tapdata.debug.DebugException;
import io.tapdata.debug.DebugFind;
import io.tapdata.debug.DebugUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugFindCache implements DebugFind {

	private final static Logger logger = LogManager.getLogger(DebugFindCache.class);

	private Job job;
	private Connections sourceConn;
	private Connections connections;
	private Stage stage;

	public DebugFindCache(Job job, Connections sourceConn, Connections connections, Stage stage) {
		this.job = job;
		this.sourceConn = sourceConn;
		this.connections = connections;
		this.stage = stage;
	}

	@Override
	public List<Map<String, Object>> backFindData(List<MessageEntity> msgs) throws DebugException {

		List<Map<String, Object>> datas = new ArrayList<>();

		return datas;
	}
}
