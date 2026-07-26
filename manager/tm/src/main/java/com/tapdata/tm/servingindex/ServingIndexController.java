package com.tapdata.tm.servingindex;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * P1-2 · 「服务型索引」读回触发端点（TAP-12057 / ADR-0009）。
 *
 * <p>本端点是<b>触发端点</b>、不是同步读回端点（ADR-0009）：非阻塞地解析目标引擎并发射
 * {@code queryIndexes} pipe，结果由引擎推回<b>发起方前端 ws 会话</b>（不在 HTTP 应答里）。前端在自己的 ws 上
 * 按 {@code connectionId + tableName + reqId} 关联，并须自备超时与「引擎不在线」态。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本包（{@code com.tapdata.tm.servingindex..}）绝不依赖 {@code MongoTemplate}
 * （由 {@code ServingIndexPackageRedlineArchTest} 编译期强制）；连接/用户解析一律走既有 service 接口。</p>
 */
@RestController
@RequestMapping({"api/serving-indexes"})
@Slf4j
public class ServingIndexController {

	private final ServingIndexService servingIndexService;
	private final DataSourceService dataSourceService;
	private final UserService userService;

	public ServingIndexController(ServingIndexService servingIndexService, DataSourceService dataSourceService,
								  UserService userService) {
		this.servingIndexService = servingIndexService;
		this.dataSourceService = dataSourceService;
		this.userService = userService;
	}

	/**
	 * 触发「按连接 + 表读回索引」。非阻塞返回；结果经引擎 ws 推回发起方前端会话（见 ADR-0009）。
	 */
	@PostMapping("query/{connectionId}")
	public ResponseMessage<Void> queryIndexes(@PathVariable("connectionId") String connectionId,
											  @RequestBody QueryIndexesRequest request) {
		DataSourceConnectionDto connectionDto = dataSourceService.findById(new ObjectId(connectionId));
		Assert.notNull(connectionDto, "connection is empty");
		UserDetail userDetail = userService.loadUserById(new ObjectId(connectionDto.getUserId()));
		servingIndexService.sendQueryIndexes(connectionDto, request.getTableName(), request.getReqId(),
				request.getClientId(), userDetail);
		return new ResponseMessage<>();
	}
}
