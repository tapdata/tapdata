package io.tapdata.inspect.compare;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.inspect.InspectDetail;
import com.tapdata.mongo.ClientMongoOperator;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 差异详情查询
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/8/2 下午8:16
 * </pre>
 */
public class DiffDetailCursor implements AutoCloseable {

	private String inspectResultId;
	private ClientMongoOperator clientMongoOperator;
	private List<String> sourceKeys;
	private List<String> targetKeys;

	private long counts = 0;
	private int pageIndex = 0;
	private int limit = 100;
	private boolean isEnd = false;
	private List<List<Object>> data;

	public DiffDetailCursor(String inspectResultId, ClientMongoOperator clientMongoOperator, List<String> sourceKeys, List<String> targetKeys) {
		this.inspectResultId = inspectResultId;
		this.clientMongoOperator = clientMongoOperator;
		this.sourceKeys = sourceKeys;
		this.targetKeys = targetKeys;

		if (null != inspectResultId && !inspectResultId.isEmpty()) {
			counts = clientMongoOperator.count(Query.query(Criteria.where("inspectResultId").regex("^" + inspectResultId + "$")), ConnectorConstant.INSPECT_DETAILS_COLLECTION);
		}
	}

	public boolean next() {
		if (isEnd) return false;

		// 非差异校验，只进循环一次
		if (diffCounts() == 0) {
			isEnd = true;
			data = null;
			return true;
		}

		// 加载差异详情
		List<InspectDetail> inspectDetails = clientMongoOperator.find(Query
						.query(Criteria.where("inspectResultId").regex("^" + inspectResultId + "$"))
						.skip(pageIndex * limit)
						.limit(limit)
				, ConnectorConstant.INSPECT_DETAILS_COLLECTION, InspectDetail.class);
		if (null == inspectDetails || inspectDetails.isEmpty()) {
			isEnd = true;
			data = null;
			return false;
		}

		List<Object> values;
		data = new ArrayList<>();
		for (InspectDetail detail : inspectDetails) {
			values = new ArrayList<>();
			if (null != detail.getSource()) {
				for (String k : sourceKeys) {
					values.add(detail.getSource().get(k));
				}
			} else {
				// 兼容高级校验的返回结果
				Map<String, Object> data = detail.getTarget();
				if (data.containsKey("data") && data.containsKey("message") && data.get("data") instanceof Map) {
					data = (Map<String, Object>) data.get("data");
					for (String k : targetKeys) {
						values.add(data.get(k));
					}
				} else {
					for (String k : targetKeys) {
						values.add(detail.getTarget().get(k));
					}
				}
			}
			data.add(values);
		}

		pageIndex++;
		return true;
	}

	public long diffCounts() {
		return counts;
	}

	public List<List<Object>> getData() {
		return data;
	}

	@Override
	public void close() throws Exception {

	}
}
