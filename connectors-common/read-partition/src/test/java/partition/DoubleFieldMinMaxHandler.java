package partition;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class DoubleFieldMinMaxHandler extends FieldMinMaxHandler {

	public DoubleFieldMinMaxHandler(List<Map<String, Object>> records) {
		super(records);
	}

	@Override
	public boolean match(QueryOperator queryOperator, Map<String, Object> record) {
		int operator = queryOperator.getOperator();
		Double recordValue = (Double) record.get(queryOperator.getKey());
		Double queryValue = (Double) queryOperator.getValue();
		switch (operator) {
			case QueryOperator.LT:
				return recordValue < queryValue;
			case QueryOperator.LTE:
				return recordValue <= queryValue;
			case QueryOperator.GT:
				return recordValue > queryValue;
			case QueryOperator.GTE:
				return recordValue >= queryValue;
		}
		return false;
	}

	@Override
	protected boolean largerThan(Object a, Object b) {
		Double aInt = (Double) a;
		Double bInt = (Double) b;
		return aInt > bInt;
	}


}
