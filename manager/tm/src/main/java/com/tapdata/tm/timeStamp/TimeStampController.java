/**
 * @title: TimeStampController
 * @description:
 * @author lk
 * @date 2021/12/1
 */
package com.tapdata.tm.timeStamp;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "TimeStamp", description = "TimeStamp相关接口")
@RestController
@RequestMapping("/api/timeStamp")
public class TimeStampController extends BaseController {

	@Operation(summary = "获取当前时间戳")
	@GetMapping
	public ResponseMessage<Long> timeStamp() {
		return success(System.currentTimeMillis());
	}
}
