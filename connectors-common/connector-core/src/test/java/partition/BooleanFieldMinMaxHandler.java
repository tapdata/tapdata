package partition;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.List;
import java.util.Map;

/**
 * @author aplomb
 */
public class BooleanFieldMinMaxHandler extends FieldMinMaxHandler {

	public BooleanFieldMinMaxHandler(List<Map<String, Object>> records) {
		super(records);
	}

	@Override
	public boolean match(QueryOperator queryOperator, Map<String, Object> record) {
		return false;
	}

	@Override
	protected boolean largerThan(Object a, Object b) {
		Boolean aInt = (Boolean) a;
		Boolean bInt = (Boolean) b;
		return aInt.compareTo(bInt) > 0;
	}

	public static void main(String[] args) {
		Boolean b1 = new Boolean(false);
		Boolean b2 = new Boolean(true);
		System.out.println(b2.compareTo(b1));
	}
}
