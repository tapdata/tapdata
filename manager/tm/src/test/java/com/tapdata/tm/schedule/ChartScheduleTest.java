package com.tapdata.tm.schedule;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChartScheduleTest {
    ChartSchedule chartSchedule;
    ChartViewService chartViewService;
    UserService userService;
    @BeforeEach
    void init() {
        chartSchedule = new ChartSchedule();
        chartViewService = mock(ChartViewService.class);
        userService = mock(UserService.class);
        chartSchedule.setChartViewService(chartViewService);
        chartSchedule.setUserService(userService);
    }

    @Nested
    class Chart6Test {
        List<UserDetail> userDetails;
        UserDetail user;
        Chart6Vo chart6Vo;
        @BeforeEach
        void init() {
            userDetails = new ArrayList<>();
            chart6Vo = Chart6Vo.builder().build();
            user = mock(UserDetail.class);
            userDetails.add(user);
            when(userService.loadAllUser()).thenReturn(userDetails);
            when(chartViewService.transmissionOverviewChartData(user)).thenReturn(chart6Vo);
            when(user.getUserId()).thenReturn("gavin-id");
        }

        @Test
        void testNormal() {
            chart6Vo.setDeletedTotal(new BigInteger(new byte[]{100, 100}));
            chartSchedule.chart6();
            Assertions.assertNotNull(ChartSchedule.cache.get("gavin-id"));
            verify(userService).loadAllUser();
            verify(chartViewService).transmissionOverviewChartData(user);
            verify(user).getUserId();
        }

        @Test
        void testVoIsNull() {
            when(chartViewService.transmissionOverviewChartData(user)).thenReturn(null);
            chartSchedule.chart6();
            Assertions.assertNull(ChartSchedule.cache.get("gavin-id"));
            verify(userService).loadAllUser();
            verify(chartViewService).transmissionOverviewChartData(user);
            verify(user).getUserId();
        }

        @Test
        void testVoIsEmpty() {
            chart6Vo.setDeletedTotal(BigInteger.ZERO);
            chart6Vo.setInputTotal(BigInteger.ZERO);
            chart6Vo.setInsertedTotal(BigInteger.ZERO);
            chart6Vo.setOutputTotal(BigInteger.ZERO);
            chart6Vo.setUpdatedTotal(BigInteger.ZERO);
            when(chartViewService.transmissionOverviewChartData(user)).thenReturn(chart6Vo);
            chartSchedule.chart6();
            Assertions.assertNull(ChartSchedule.cache.get("gavin-id"));
            verify(userService).loadAllUser();
            verify(chartViewService).transmissionOverviewChartData(user);
            verify(user).getUserId();
        }
    }

    @AfterEach
    void close() {
        ChartSchedule.cache.clear();
    }
}