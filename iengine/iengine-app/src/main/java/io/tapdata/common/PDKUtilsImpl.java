package io.tapdata.common;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.error.CoreException;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.modules.api.net.error.NetErrors;
import io.tapdata.modules.api.pdk.PDKUtils;

import java.util.Map;

@Implementation(PDKUtils.class)
public class PDKUtilsImpl implements PDKUtils {

	@Override
	public Map<String, Object> getConnectionConfig(String connectionId) {
		if(connectionId == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "connectionId is null");
		Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
		if(connections == null)
			throw new CoreException(NetErrors.CONNECTIONS_NOT_FOUND, "Connections {} not found", connectionId);

		return connections.getConfig();
	}

	@Override
	public PDKInfo downloadPdkFileIfNeed(String pdkHash) {
		ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
		//TODO should optimize to cache pdkHash -> databaseType.
		DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, pdkHash);
		if(databaseType == null) {
			throw new CoreException(NetErrors.PDK_HASH_NOT_FOUND, "PDK hash {} can not be found", pdkHash);
		}
		if(databaseType.getPdkId() == null || databaseType.getGroup() == null || databaseType.getVersion() == null || databaseType.getJarFile() == null)
			throw new CoreException(NetErrors.ILLEGAL_PARAMETERS, "Illegal arguments, pdkId {}, group {}, version {}, jarFile {}", databaseType.getPdkId(), databaseType.getGroup(), databaseType.getVersion(), databaseType.getJarFile());
		if(clientMongoOperator instanceof HttpClientMongoOperator)
			PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, pdkHash, databaseType.getJarFile(), databaseType.getJarRid());
		else
			throw new CoreException(NetErrors.UNEXPECTED_MONGO_OPERATOR, "Unexpected mongodb operator");
		return new PDKInfo().pdkId(databaseType.getPdkId()).group(databaseType.getGroup()).version(databaseType.getVersion());
	}
}
