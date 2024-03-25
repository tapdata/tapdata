package com.tapdata.tm.customer.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customer.dto.CustomerDto;
import com.tapdata.tm.customer.service.CustomerService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MessageUtil;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Locale;

/**
 * @Date: 2022/02/14
 * @Description:
 */
@Tag(name = "Customer", description = "Customer相关接口")
@RestController
@RequestMapping("/api/Customer")
@Slf4j
public class CustomerController extends BaseController {

    @Autowired
    private CustomerService customerService;

    @Autowired
    private UserLogService userLogService;

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param customer
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping
    public ResponseMessage<CustomerDto> update(@RequestBody @Validated CustomerDto customer) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
        HttpServletRequest request = attributes.getRequest();
        Locale locale = WebUtils.getLocale(request);
        userLogService.addUserLog(Modular.CUSTOMER, com.tapdata.tm.userLog.constant.Operation.UPDATE, getLoginUser(), MessageUtil.getMessage(locale,"EnterpriseInformation"));

        //customer.setId(new ObjectId(getLoginUser().getCustomerId()));
        long result = customerService.upsert(
                Query.query(Criteria.where("_id").is(new ObjectId(getLoginUser().getCustomerId())))
                , customer);

        log.debug("Upsert custom " + result);

        return success(customer);
    }

    @Operation(summary = "Get customer info for current user.")
    @GetMapping
    public ResponseMessage<CustomerDto> get() {
        UserDetail user = getLoginUser();
        CustomerDto customer = customerService.findOne(Query.query(Criteria.where("id").is(new ObjectId(user.getCustomerId()))));
        if (customer == null) {
            customer = new CustomerDto();
            customer.setCustomId(user.getCustomerId());
            customer.setId(new ObjectId(user.getCustomerId()));
        }
        return success(customer);
    }
}
