package com.tapdata.tm.role.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.repository.RoleRepository;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RoleServiceTest {
    private RoleService roleService;
    private RoleRepository repository;
    @BeforeEach
    void beforeEach() {
        roleService = mock(RoleService.class);
        repository = mock(RoleRepository.class);
        ReflectionTestUtils.setField(roleService, "repository", repository);
    }
    @Test
    void testUpdateByWhere() {
        when(repository.filterToQuery(any(Filter.class))).thenReturn(mock(Query.class));
        when(repository.update(any(Query.class), any(Update.class), any(UserDetail.class))).thenReturn(mock(UpdateResult.class));
        doCallRealMethod().when(roleService).updateByWhere(any(Where.class), any(Document.class), any(UserDetail.class));
        roleService.updateByWhere(Where.where("test", "11"), new Document(), mock(UserDetail.class));
        verify(repository, new Times(1)).update(any(Query.class), any(Update.class), any(UserDetail.class));
    }
}
