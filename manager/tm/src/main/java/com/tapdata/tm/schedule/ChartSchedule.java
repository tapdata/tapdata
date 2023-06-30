package com.tapdata.tm.schedule;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Setter(onMethod_ = {@Autowired})
public class ChartSchedule {


    public static Map<String, Chart6Vo> cache = new HashMap<>();

    private TaskService taskService;

    private UserService userService;

    @Scheduled(fixedDelay = 60000)
    public void chart6() {
        List<UserDetail> userDetails = userService.loadAllUser();
        for (UserDetail userDetail : userDetails) {
            Chart6Vo chart6Vo = taskService.chart6(userDetail);
            put(userDetail.getUserId(), chart6Vo);
        }
    }


    public static void  put(String key, Chart6Vo chart6Vo) {
        if (chart6Vo != null && !chart6Vo.empty()) {
            cache.put(key, chart6Vo);
        }
    }
}
