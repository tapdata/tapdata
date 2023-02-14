package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tapdata.entity.Connections;
import com.tapdata.tm.commons.dag.Node;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 任务编排cache配置
 *
 * @author jackin
 */
public class DataFlowCacheConfig implements Serializable {

	private static final long serialVersionUID = 6892135166905479192L;

	private String cacheKeys;

	private String cacheName;

	private String cacheType;

	/**
	 * 最大行数
	 */
	private long maxRows;

	/**
	 * 最大容量，单位：mb
	 */
	private long maxSize;

	/**
	 * 失效时间
	 */
	private long ttl;

	/**
	 * 缓存字段
	 */
	private Set<String> fields;

	/**
	 * 缓存数据来源
	 */
	@JsonIgnore
	private Connections sourceConnection;

	private String sourceConnectionId;

	/**
	 * 源节点的node信息
	 */
	private Node sourceNode;

	private Node cacheNode;

	/**
	 * 缓存的源表名称
	 */
	private String tableName;

	private Stage sourceStage;

	private List<String> primaryKeys;

	private String externalStorageId;

	public DataFlowCacheConfig() {
	}

	public DataFlowCacheConfig(
			String cacheKeys,
			String cacheName,
			String cacheType,
			long maxRows,
			long maxSize,
			long ttl,
			Set<String> fields,
			Connections sourceConnection,
			Node sourceNode,
			String tableName,
			Stage sourceStage,
			List<String> primaryKeys
	) {

		this.cacheKeys = cacheKeys;
		this.cacheName = cacheName;
		this.cacheType = cacheType;
		this.maxRows = maxRows;
		this.maxSize = maxSize;
		this.ttl = ttl;
		this.fields = fields;
		this.sourceConnection = sourceConnection;
		this.sourceNode = sourceNode;
		this.tableName = tableName;
		this.sourceStage = sourceStage;
		if (CollectionUtils.isNotEmpty(primaryKeys)) {
			this.primaryKeys = primaryKeys;
		} else {
			this.primaryKeys = Collections.singletonList("_id");
		}
		if (this.sourceConnection != null) {
			this.sourceConnectionId = sourceConnection.getId();
		}
	}

	public Stage getSourceStage() {
		return sourceStage;
	}

	public void setSourceStage(Stage sourceStage) {
		this.sourceStage = sourceStage;
	}

	public String getCacheKeys() {
		return cacheKeys;
	}

	public void setCacheKeys(String cacheKeys) {
		this.cacheKeys = cacheKeys;
	}

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	public String getCacheType() {
		return cacheType;
	}

	public void setCacheType(String cacheType) {
		this.cacheType = cacheType;
	}

	public long getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(long maxRows) {
		this.maxRows = maxRows;
	}

	public long getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}

	public Connections getSourceConnection() {
		return sourceConnection;
	}

	public void setSourceConnection(Connections sourceConnection) {
		this.sourceConnection = sourceConnection;
		if (sourceConnection != null) {
			this.sourceConnectionId = sourceConnection.getId();
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(List<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}

	public String getSourceConnectionId() {
		return sourceConnectionId;
	}

	public void setSourceConnectionId(String sourceConnectionId) {
		this.sourceConnectionId = sourceConnectionId;
	}

	public Node getSourceNode() {
		return sourceNode;
	}

	public void setSourceNode(Node sourceNode) {
		this.sourceNode = sourceNode;
	}

	public Node getCacheNode() {
		return cacheNode;
	}

	public void setCacheNode(Node cacheNode) {
		this.cacheNode = cacheNode;
	}

	public String getExternalStorageId() {
		return externalStorageId;
	}

	public void setExternalStorageId(String externalStorageId) {
		this.externalStorageId = externalStorageId;
	}

	@Override
	public String toString() {
		return "DataFlowCacheConfig{" +
				"cacheKeys='" + cacheKeys + '\'' +
				", cacheName='" + cacheName + '\'' +
				", maxRows=" + maxRows +
				", maxSize=" + maxSize +
				", sourceConnection=" + sourceConnection +
				", tableName='" + tableName + '\'' +
				", sourceStage=" + sourceStage +
				'}';
	}
}
