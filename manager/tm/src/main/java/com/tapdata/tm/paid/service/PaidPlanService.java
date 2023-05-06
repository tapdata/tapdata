package com.tapdata.tm.paid.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.paid.dto.CheckPaidPlanRes;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.tcm.dto.PaidPlanRes;
import com.tapdata.tm.tcm.service.TcmService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/11/22 下午7:42
 */
@Service
@Profile("dfs")
@Deprecated
public class PaidPlanService {

    @Autowired
    private TcmService tcmService;

    @Autowired
    private TaskService taskService;

    /**
     * 检查用户付费计划是否有效
     * @param loginUser
     * @return
     */
    public CheckPaidPlanRes checkPaidPlan(UserDetail loginUser) {

        PaidPlanRes paidPlan = tcmService.describeUserPaidPlan(loginUser.getExternalUserId());

        Map<String, Integer> limit = paidPlan != null ? paidPlan.getLimit() : null;

        Integer pipelineLimit = limit != null ? limit.get("pipelines") : null;

        if (pipelineLimit == null) {
            pipelineLimit = 3;
        }

        long count = taskService.count(Query.query(
                Criteria.where("is_deleted").ne(true)
                        .and("status").nin(Arrays.asList("deleting", "delete_failed"))), loginUser);

        CheckPaidPlanRes result = new CheckPaidPlanRes();

        Map<String, String> message = new HashMap<>();
        boolean isValid = pipelineLimit == -1 || count < pipelineLimit;

        if (!isValid) {
            message.put("pipelines", "Insufficient effective quota");
        }
        result.setValid(isValid);
        result.setMessages(message);
        result.setLimit(pipelineLimit);
        result.setCurrent(count);
        return result;
    }
}
