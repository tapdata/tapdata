package io.tapdata;

import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.ConnectionsType;
import io.tapdata.entity.LoadSchemaResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-01-26 12:06
 **/
public class BaseConnection implements TapInterface {

	protected List<BaseConnectionValidateResultDetail> details;

	@Override
	public Map<String, Boolean> getSupported(String[] supports) {
		return null;
	}

	@Override
	public List<BaseConnectionValidateResultDetail> connectionsInit(ConnectionsType connectionsType) {
		details = new ArrayList<>();
		return details;
	}

	@Override
	public BaseConnectionValidateResult testConnections(Connections connections) {
		BaseConnectionValidateResult result = new BaseConnectionValidateResult();
		result.setValidateResultDetails(details);
		result.setStatus(BaseConnectionValidateResult.CONNECTION_STATUS_READY);
		return result;
	}

	@Override
	public LoadSchemaResult<RelateDataBaseTable> loadSchema(Connections connections) {
		return new LoadSchemaResult<>();
	}
}
