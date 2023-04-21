package com.tapdata.tm.paid.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.paid.dto.CheckPaidPlanRes;
import com.tapdata.tm.paid.service.PaidPlanService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/11/22 下午7:31
 */
@RestControllerAdvice
@RequestMapping("/api/paid")
@Profile("dfs")
@Deprecated
public class PaidPlanController extends BaseController {

    @Autowired
    PaidPlanService paidPlanService;

    @GetMapping("/plan")
    public ResponseMessage<CheckPaidPlanRes> checkPaidPlan() {

        return success(paidPlanService.checkPaidPlan(getLoginUser()));

    }
}
