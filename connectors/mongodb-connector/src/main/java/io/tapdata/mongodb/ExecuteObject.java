package io.tapdata.mongodb;

import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.exception.NotSupportedException;

import java.util.List;
import java.util.Map;

public class ExecuteObject {

	public static final String INSERT_OP = "insert";
	public static final String UPDATE_OP = "update";
	public static final String DELETE_OP = "delete";

	public static final String AGGREGATE_OP = "aggregate";
	public static final String FIND_AND_MODIFY_OP = "findAndModify";

	private String op;

	private String sql;

	private String database;

	private String collection;

	private Map<String, Object> filter;

	private Map<String, Object> opObject;

	private Map<String, Object> sort;

	private List<Map<String, Object>> pipeline;

	private int limit;
	private int skip;

	private boolean upsert;

	private boolean multi;

	private Map<String, Object> projection;

	private int batchSize;

	public ExecuteObject(Map<String, Object> executeObj) {
		this.op = executeObj.get("op") == null ? null : executeObj.get("op").toString();
		this.sql = executeObj.get("sql") == null ? null : executeObj.get("sql").toString();
		this.database = executeObj.get("database") == null ? null : executeObj.get("database").toString();
		this.collection = executeObj.get("collection") == null ? null : executeObj.get("collection").toString();
		this.filter = getFilter(executeObj.get("filter"));
		this.opObject = executeObj.get("opObject") == null ? null : (Map<String, Object>) executeObj.get("opObject");
		this.upsert = executeObj.get("upsert") == null ? false : Boolean.valueOf(executeObj.get("upsert").toString());
		this.multi = executeObj.get("multi") == null ? false : Boolean.valueOf(executeObj.get("multi").toString());
		this.sort = executeObj.get("sort") == null ? null : (Map<String, Object>) executeObj.get("sort");
		this.limit = executeObj.get("limit") == null ? 0 : (int) executeObj.get("limit");
		this.skip = executeObj.get("skip") == null ? 0 : (int) executeObj.get("skip");
		this.projection = executeObj.get("projection") == null ? null : (Map<String, Object>) executeObj.get("projection");
		this.pipeline = getPipeline(executeObj.get("pipeline"));
		this.batchSize = executeObj.get("batchSize") == null ? 1000 : (int) executeObj.get("batchSize");
	}

	private Map<String, Object> getFilter(Object obj) {
		if (obj != null) {
			if (obj instanceof Map) {
				return (Map<String, Object>) obj;
			} else if (obj instanceof String) {
				return TapSimplify.fromJson((String) obj, Map.class);
			} else {
				throw new NotSupportedException(obj.toString());
			}
		}
		return null;
	}

	private List<Map<String, Object>> getPipeline(Object obj) {
		if (obj != null) {
			if (obj instanceof List) {
				return (List<Map<String, Object>>) obj;
			} else if (obj instanceof String) {
				return TapSimplify.fromJson((String) obj, List.class);
			} else {
				throw new NotSupportedException(obj.toString());
			}
		}
		return null;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public Map<String, Object> getFilter() {
		return filter;
	}

	public void setFilter(Map<String, Object> filter) {
		this.filter = filter;
	}

	public Map<String, Object> getOpObject() {
		return opObject;
	}

	public void setOpObject(Map<String, Object> opObject) {
		this.opObject = opObject;
	}

	public boolean isUpsert() {
		return upsert;
	}

	public void setUpsert(boolean upsert) {
		this.upsert = upsert;
	}

	public boolean isMulti() {
		return multi;
	}

	public void setMulti(boolean multi) {
		this.multi = multi;
	}

	public Map<String, Object> getSort() {
		return sort;
	}

	public void setSort(Map<String, Object> sort) {
		this.sort = sort;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getSkip() {
		return skip;
	}

	public void setSkip(int skip) {
		this.skip = skip;
	}

	public Map<String, Object> getProjection() {
		return projection;
	}

	public void setProjection(Map<String, Object> projection) {
		this.projection = projection;
	}

	public List<Map<String, Object>> getPipeline() {
		return pipeline;
	}

	public void setPipeline(List<Map<String, Object>> pipeline) {
		this.pipeline = pipeline;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public String toString() {
		return "ExecuteObject{" +
						"op='" + op + '\'' +
						", sql='" + sql + '\'' +
						", database='" + database + '\'' +
						", collection='" + collection + '\'' +
						", filter=" + filter +
						", opObject=" + opObject +
						", sort=" + sort +
						", pipeline=" + pipeline +
						", limit=" + limit +
						", skip=" + skip +
						", upsert=" + upsert +
						", multi=" + multi +
						", projection=" + projection +
						", batchSize=" + batchSize +
						'}';
	}
}
