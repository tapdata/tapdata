package com.tapdata.tm.init.patches.dfs;

import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.repository.DataSourceRepository;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.init.patches.AbsPatch;
import com.tapdata.tm.init.patches.PatchAnnotation;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import io.tapdata.utils.AppType;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.List;
import java.util.Map;

@PatchAnnotation(appType = AppType.DFS, version = "3.5-4")
public class v3_5_4_Heartbeat_Dummy_Connection extends AbsPatch {
	private DataSourceRepository dataSourceRepository;

	public v3_5_4_Heartbeat_Dummy_Connection(PatchType type, PatchVersion version) {
		super(type, version);
		dataSourceRepository = SpringContextHelper.getBean(DataSourceRepository.class);
	}

	@Override
	public void run() {
		Query query = Query.query(Criteria.where("name").is(ConnHeartbeatUtils.CONNECTION_NAME));
		List<DataSourceEntity> connections = dataSourceRepository.findAll(query);
		if (CollectionUtils.isEmpty(connections)) {
			return;
		}
		for (DataSourceEntity conn : connections) {
			dataSourceRepository.decryptConfig(conn);
			Map<String, Object> config = conn.getConfig();
			if (MapUtils.isEmpty(config) || !ConnHeartbeatUtils.MODE.equals(config.get("mode"))) {
				continue;
			}
			config.put("incremental_types", new int[]{1});
			dataSourceRepository.encryptConfig(conn);
			query = Query.query(Criteria.where("_id").is(conn.getId()));
			Update update = Update.update("encryptConfig", conn.getEncryptConfig());
			dataSourceRepository.update(query, update);
		}
	}
}
