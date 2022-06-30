/**
 * @title: JobStatusController
 * @description:
 * @author lk
 * @date 2021/7/19
 */
package com.tapdata.job;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/api/v1/status")
@RestController
public class JobStatusController extends BaseController {

	@Autowired
	private JobStatusService jobStatusService;

	@RequestMapping("/test/{id}/{target}")
	public ResponseMessage xxx(@PathVariable("id") String id, @PathVariable("target") String target) throws Exception {
		jobStatusService.xxx(id, target);
		return success(null);
	}
}
