package partition;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class DateTimeFieldMinMaxHandler extends FieldMinMaxHandler {

	public DateTimeFieldMinMaxHandler(List<Map<String, Object>> records) {
		super(records);
	}

	@Override
	public boolean match(QueryOperator queryOperator, Map<String, Object> record) {
		int operator = queryOperator.getOperator();
		DateTime recordValue = AnyTimeToDateTime.toDateTime(record.get(queryOperator.getKey()));
		DateTime queryValue = AnyTimeToDateTime.toDateTime(queryOperator.getValue());
		switch (operator) {
			case QueryOperator.LT:
				return recordValue.toNanoSeconds().compareTo(queryValue.toNanoSeconds()) < 0;
			case QueryOperator.LTE:
				return recordValue.toNanoSeconds().compareTo(queryValue.toNanoSeconds()) <= 0;
			case QueryOperator.GT:
				return recordValue.toNanoSeconds().compareTo(queryValue.toNanoSeconds()) > 0;
			case QueryOperator.GTE:
				return recordValue.toNanoSeconds().compareTo(queryValue.toNanoSeconds()) >= 0;
		}
		return false;
	}

	@Override
	protected boolean largerThan(Object a, Object b) {
		DateTime aInt = AnyTimeToDateTime.toDateTime(a);
		DateTime bInt = AnyTimeToDateTime.toDateTime(b);
		return aInt.toNanoSeconds().compareTo(bInt.toNanoSeconds()) > 0;
	}


}
