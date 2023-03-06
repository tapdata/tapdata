package partition;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class IntFieldMinMaxHandler extends FieldMinMaxHandler {

	public IntFieldMinMaxHandler(List<Map<String, Object>> records) {
		super(records);
	}

	@Override
	public boolean match(QueryOperator queryOperator, Map<String, Object> record) {
		int operator = queryOperator.getOperator();
		Integer recordValue = (Integer) record.get(queryOperator.getKey());
		if(recordValue == null)
			recordValue = Integer.MIN_VALUE;
		Integer queryValue = (Integer) queryOperator.getValue();
		if(queryValue == null)
			queryValue = Integer.MIN_VALUE;
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
		Integer aInt = (Integer) a;
		if(aInt == null)
			aInt = Integer.MIN_VALUE;
		Integer bInt = (Integer) b;
		if(bInt == null)
			bInt = Integer.MIN_VALUE;
		return aInt > bInt;
	}


}
