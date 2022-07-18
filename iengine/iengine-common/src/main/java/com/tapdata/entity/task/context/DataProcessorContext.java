package com.tapdata.entity.task.context;

import com.tapdata.cache.ICacheService;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;

import java.io.Serializable;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-03-17 14:22
 **/
public class DataProcessorContext extends ProcessorBaseContext implements Serializable {

	private static final long serialVersionUID = 2102419458896199742L;

	private final Connections connections;
	private final Connections sourceConn;
	private final Connections targetConn;
	private final ICacheService cacheService;
	private final Map<String, Object> connectionConfig;
	private final DatabaseTypeEnum.DatabaseType databaseType;

	public Connections getSourceConn() {
		return sourceConn;
	}

	public Connections getTargetConn() {
		return targetConn;
	}

	public ICacheService getCacheService() {
		return cacheService;
	}

	public Connections getConnections() {
		return connections;
	}

	public Map<String, Object> getConnectionConfig() {
		return connectionConfig;
	}

	public DatabaseTypeEnum.DatabaseType getDatabaseType() {
		return databaseType;
	}

	private DataProcessorContext(DataProcessorContextBuilder builder) {
		super(builder);
		sourceConn = builder.sourceConn;
		targetConn = builder.targetConn;
		cacheService = builder.cacheService;
		connections = builder.connections;
		connectionConfig = builder.connectionConfig;
		databaseType = builder.databaseType;
	}

	public static DataProcessorContextBuilder newBuilder() {
		return new DataProcessorContextBuilder();
	}

	public static class DataProcessorContextBuilder extends ProcessorBaseContextBuilder<DataProcessorContextBuilder> {
		private Connections connections;
		private Connections sourceConn;
		private Connections targetConn;
		private ICacheService cacheService;
		private Map<String, Object> connectionConfig;
		private DatabaseTypeEnum.DatabaseType databaseType;

		public DataProcessorContextBuilder() {
		}

		public DataProcessorContextBuilder withConnections(Connections val) {
			connections = val;
			return this;
		}

		public DataProcessorContextBuilder withSourceConn(Connections val) {
			sourceConn = val;
			return this;
		}

		public DataProcessorContextBuilder withTargetConn(Connections val) {
			targetConn = val;
			return this;
		}

		public DataProcessorContextBuilder withCacheService(ICacheService val) {
			this.cacheService = val;
			return this;
		}

		public DataProcessorContextBuilder withConnectionConfig(Map<String, Object> val) {
			this.connectionConfig = val;
			return this;
		}

		public DataProcessorContextBuilder withDatabaseType(DatabaseTypeEnum.DatabaseType val) {
			this.databaseType = val;
			return this;
		}

		public DataProcessorContext build() {
			return new DataProcessorContext(this);
		}
	}
}
