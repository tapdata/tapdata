/*
  @title: DataFlowController
 * @description:
 * @author Steven
 * @date 2021/12/6
 */
package com.tapdata.tm.CustomerJobLogs.controller;

import com.tapdata.tm.CustomerJobLogs.dto.CustomerJobLogsDto;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.common.logging.error.ErrorCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import io.tapdata.common.logging.format.*;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.*;

@Tag(name = "CustomerJobLogs", description = "CustomerJobLogs api")
@RestController
@RequestMapping(value = {"/api/CustomerJobLogs"})
public class CustomerJobLogsController extends BaseController {

	@Autowired
	private CustomerJobLogsService customerJobLogsService;

	@Operation(summary = "Find all Customer Job Logs of the model matched by filter from the data source")
	@GetMapping
	public ResponseMessage<Page<CustomerJobLogsDto>> find(
			@Parameter(in = ParameterIn.QUERY, description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`).")
			@RequestParam(value = "filter", required = false) String filterJson) {
		Filter filter = parseFilter(filterJson);
		if (filter == null) {
			filter = new Filter();
		}
		if (filter.getSort() == null) {
			filter.setSort(new ArrayList<>());
		}
		if(filter.getSort().stream().noneMatch(s -> s.startsWith("createAt"))) {
			filter.getSort().add("createAt DESC");
		}
		return success(customerJobLogsService.find(filter, getLoginUser()));
	}

	@Operation(summary = "Insert a new one into the CustomerJobLogs")
	@PostMapping
	public ResponseMessage<Object> post(@RequestBody CustomerJobLogsDto dto) {
		return success(customerJobLogsService.save(dto, getLoginUser()));
	}

	@Operation(summary = "Find a solutions")
	@GetMapping("/solutions")
	public ResponseMessage<Object> solutions(@RequestParam(value = "code", required = false) Integer code) {
		Locale locale = null;
		try {
			RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
			if (requestAttributes != null){
				locale = WebUtils.getLocale(((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest());
			}
		}catch (Exception ignored){

		}
		if (locale == null){
			locale = Locale.getDefault();
		}
		String TMLocale;
		String a = locale.toString();
		switch (a) {
			case "zh_CN":
				TMLocale = "zh-cn";
				break;
			case "zh_TW":
				TMLocale = "zh-tw";
				break;
			default:
				TMLocale = "en";//throw new InvalidParameterException("un-supported locale setting for error code solutions.1");
		}
		if(code != null) {
			CustomerMessage customerMessage = null;
			if((code >= 10000 && code < 60000) || (code >= 90000 && code <= 100000)) {
				customerMessage = CustomerMessageFactory.getCustomerMessages("agent",1);
			}else if(code >= 60000 && code < 70000){
				customerMessage = CustomerMessageFactory.getCustomerMessages("tm",1);
			}
			assert customerMessage != null;
			ErrorCode errorCode = customerMessage.getErrorCodeSolutions(code,TMLocale);//agentCustomerMessagesV1.getErrorCodeSolutions(code, "zh-cn");
			ArrayList<ErrorCode> arr = new ArrayList<>();
			if(errorCode != null){
				arr.add(errorCode);
			}
			return success(arr);
		}else{
			CustomerMessage customerMessage = CustomerMessageFactory.getCustomerMessages("agent",1);
			CustomerMessage customerMessageTM = CustomerMessageFactory.getCustomerMessages("tm",1);
			ArrayList<ErrorCode> customerMessageList = customerMessage.getAllErrorCodeSolutions(TMLocale);
			ArrayList<ErrorCode> customerMessageTMList = customerMessageTM.getAllErrorCodeSolutions(TMLocale);
			customerMessageList.addAll(customerMessageTMList);
			return success(customerMessageList);
		}
	}

}
