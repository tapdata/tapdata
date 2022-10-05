/**
 * @title: DataFlowStateController
 * @description:
 * @author lk
 * @date 2021/11/15
 */
package com.tapdata.tm.statemachine.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.statemachine.dto.StateMachineDto;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/state")
public class StateMachineController extends BaseController {

	@Autowired
	private StateMachineService stateMachineService;

	@PostMapping("/dataFlow/{event}")
	public Object dataFlow(@PathVariable("event") String event, @RequestBody StateMachineDto dto){
		if (dto == null || CollectionUtils.isEmpty(dto.getIds())){
			return failed("InvalidParameter", "ids is empty");
		}
		return success(dto.getIds().stream().map(id -> getExecResult(id, event)).collect(Collectors.toList()));
	}

	@GetMapping("/task/{id}/{event}")
	public Object task(@PathVariable("id") String id, @PathVariable("event") String event){

		return success(stateMachineService.executeAboutTask(id, event, getLoginUser()));
	}

	private StateMachineResult getExecResult(String id, String event) {
		StateMachineResult result;
		try {
			result = stateMachineService.executeAboutDataFlow(id, event, getLoginUser());
		}catch (Exception e){
			result = StateMachineResult.fail(e.getMessage());
		}
		return result;
	}

}
