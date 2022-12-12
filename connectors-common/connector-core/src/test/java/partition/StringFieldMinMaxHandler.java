package partition;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class StringFieldMinMaxHandler extends FieldMinMaxHandler {

	public StringFieldMinMaxHandler(List<Map<String, Object>> records) {
		super(records);
	}

	@Override
	public boolean match(QueryOperator queryOperator, Map<String, Object> record) {
		int operator = queryOperator.getOperator();
		String recordValue = (String) record.get(queryOperator.getKey());
		String queryValue = (String) queryOperator.getValue();
		switch (operator) {
			case QueryOperator.LT:
				return recordValue.compareTo(queryValue) < 0;
			case QueryOperator.LTE:
				return recordValue.compareTo(queryValue) <= 0;
			case QueryOperator.GT:
				return recordValue.compareTo(queryValue) > 0;
			case QueryOperator.GTE:
				return recordValue.compareTo(queryValue) >= 0;
		}
		return false;
	}

	@Override
	protected boolean largerThan(Object a, Object b) {
		String aInt = (String) a;
		String bInt = (String) b;
		return aInt.compareTo(bInt) > 0;
	}


}
