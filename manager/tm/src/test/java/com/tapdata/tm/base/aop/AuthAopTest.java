package com.tapdata.tm.base.aop;

import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.bean.SourceDto;
import com.tapdata.tm.config.component.ProductComponent;
import com.tapdata.tm.config.security.UserDetail;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuthAopTest {

    private AuthAop authAop;
    private ProductComponent productComponent;
    private ProceedingJoinPoint pjp;
    private UserDetail userDetail;

    @BeforeEach
    void setUp() throws Throwable {
        authAop = new AuthAop();
        productComponent = mock(ProductComponent.class);
        ReflectionTestUtils.setField(authAop, "productComponent", productComponent);
        pjp = mock(ProceedingJoinPoint.class);
        when(pjp.proceed()).thenReturn("proceeded");
        userDetail = new UserDetail("user-1", "customer-1", "alice", "password", "customerType", List.of());
    }

    @Nested
    @DisplayName("Method metadataInstancesService_bulkUpsetByWhere test")
    class BulkUpsetByWhereTest {

        @Test
        @DisplayName("non-Cloud empty list does not throw and proceeds")
        void emptyListDoesNotThrowAndProceeds() throws Throwable {
            when(productComponent.isCloud()).thenReturn(false);

            Object result = assertDoesNotThrow(() ->
                    authAop.metadataInstancesService_bulkUpsetByWhere(pjp, Collections.emptyList(), userDetail));

            assertEquals("proceeded", result);
            verify(pjp, times(1)).proceed();
            assertEquals("user-1", userDetail.getUserId());
            assertEquals("alice", userDetail.getUsername());
        }

        @Test
        @DisplayName("non-Cloud non-empty list still setUserId from first model then proceeds")
        void nonEmptyListSetsUserFromFirstModelThenProceeds() throws Throwable {
            when(productComponent.isCloud()).thenReturn(false);
            SourceDto source = new SourceDto();
            source.setUser_id("owner-2");
            source.setCreateUser("bob");
            MetadataInstancesDto model = new MetadataInstancesDto();
            model.setSource(source);
            List<MetadataInstancesDto> models = new ArrayList<>();
            models.add(model);

            when(pjp.proceed()).thenAnswer(invocation -> {
                assertEquals("owner-2", userDetail.getUserId());
                assertEquals("bob", userDetail.getUsername());
                return "proceeded";
            });

            Object result = assertDoesNotThrow(() ->
                    authAop.metadataInstancesService_bulkUpsetByWhere(pjp, models, userDetail));

            assertEquals("proceeded", result);
            verify(pjp, times(1)).proceed();
            assertEquals("user-1", userDetail.getUserId());
            assertEquals("alice", userDetail.getUsername());
        }
    }
}
