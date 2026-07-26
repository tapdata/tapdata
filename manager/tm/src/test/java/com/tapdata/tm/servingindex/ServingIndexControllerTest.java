package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P1-2 · 索引读回触发端点（{@link ServingIndexController}）。
 * 只锚定编排契约：按 connectionId 解析连接与发起用户 → 委派 {@link ServingIndexService} 发射触发消息。
 */
class ServingIndexControllerTest {

	private ServingIndexService servingIndexService;
	private DataSourceService dataSourceService;
	private UserService userService;
	private ServingIndexController controller;

	@BeforeEach
	void setUp() {
		servingIndexService = mock(ServingIndexService.class);
		dataSourceService = mock(DataSourceService.class);
		userService = mock(UserService.class);
		controller = new ServingIndexController(servingIndexService, dataSourceService, userService);
	}

	@Test
	@DisplayName("按 connectionId 解析连接与用户，委派 ServingIndexService 发射触发（连 sender=clientId、reqId 透传）")
	void queryIndexes_resolvesConnectionAndUser_thenDelegates() {
		ObjectId connId = new ObjectId();
		DataSourceConnectionDto conn = mock(DataSourceConnectionDto.class);
		when(conn.getUserId()).thenReturn(new ObjectId().toHexString());
		when(dataSourceService.findById(any(ObjectId.class))).thenReturn(conn);
		UserDetail user = mock(UserDetail.class);
		when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

		QueryIndexesRequest req = new QueryIndexesRequest();
		req.setTableName("orders");
		req.setReqId("r-1");
		req.setClientId("fe-session-9");

		controller.queryIndexes(connId.toHexString(), req);

		verify(servingIndexService).sendQueryIndexes(conn, "orders", "r-1", "fe-session-9", user);
	}
}
