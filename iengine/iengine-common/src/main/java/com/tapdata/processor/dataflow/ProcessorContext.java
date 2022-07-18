package com.tapdata.processor.dataflow;

import com.tapdata.cache.ICacheService;
import com.tapdata.entity.Connections;
import com.tapdata.entity.JavaScriptFunctions;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.processor.ScriptConnection;
import com.tapdata.processor.ScriptUtil;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author jackin
 */
public class ProcessorContext {

	private volatile ScriptConnection sourceScriptConnection;

	private volatile ScriptConnection targetScriptConnection;

	private ClientMongoOperator clientMongoOperator;

	private Job job;

	private Connections sourceConn;

	private Connections targetConn;

	private List<JavaScriptFunctions> javaScriptFunctions;

	private ICacheService cacheService;

	private Consumer<List<MessageEntity>> processorHandle;

	public ProcessorContext(
			Connections sourceConn,
			Connections targetConn,
			Job job,
			ClientMongoOperator clientMongoOperator,
			List<JavaScriptFunctions> javaScriptFunctions,
			Consumer<List<MessageEntity>> processorHandle,
			ICacheService cacheService
	) {
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.job = job;
		this.clientMongoOperator = clientMongoOperator;
		this.javaScriptFunctions = javaScriptFunctions;
		this.processorHandle = processorHandle;
		this.cacheService = cacheService;
	}

	public ScriptConnection getSourceScriptConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (sourceScriptConnection == null) {
			synchronized (this) {
				if (sourceScriptConnection == null) {
					this.sourceScriptConnection = ScriptUtil.initScriptConnection(sourceConn);
				}
			}
		}
		return sourceScriptConnection;
	}

	public ScriptConnection getTargetScriptConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (targetScriptConnection == null) {
			synchronized (this) {
				if (targetScriptConnection == null) {
					this.targetScriptConnection = ScriptUtil.initScriptConnection(targetConn);
				}
			}
		}
		return targetScriptConnection;
	}

	public ICacheService getCacheService() {
		return cacheService;
	}

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public Job getJob() {
		return job;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public List<JavaScriptFunctions> getJavaScriptFunctions() {
		return javaScriptFunctions;
	}

	public Consumer<List<MessageEntity>> getProcessorHandle() {
		return processorHandle;
	}

	public void destroy() {
		if (sourceScriptConnection != null) {
			sourceScriptConnection.close();
		}
		if (targetScriptConnection != null) {
			targetScriptConnection.close();
		}
	}
}
