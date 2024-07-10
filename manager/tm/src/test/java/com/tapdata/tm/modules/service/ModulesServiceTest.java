package com.tapdata.tm.modules.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesPermissionsDto;
import com.tapdata.tm.modules.dto.ModulesTagsDto;
import com.tapdata.tm.modules.repository.ModulesRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.mockito.Mockito.*;

public class ModulesServiceTest {
    ModulesService modulesService;
    ModulesRepository modulesRepository;
    @BeforeEach
    void init(){
        modulesRepository = mock(ModulesRepository.class);
        modulesService = new ModulesService(modulesRepository);
    }
    @Nested
    class UpdatePermissionsTest{
        @Test
        void test_main(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            modulesPermissionsDto.setAcl(Arrays.asList("admin"));
            modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_aclIsNull(){
            ModulesPermissionsDto modulesPermissionsDto = new ModulesPermissionsDto();
            modulesPermissionsDto.setModuleId("test");
            Assertions.assertThrows(BizException.class,()->modulesService.updatePermissions(modulesPermissionsDto,mock(UserDetail.class)));
        }

    }

    @Nested
    class UpdateTagsTest{
        @Test
        void test_main(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            modulesTagsDto.setListtags(Arrays.asList(new Tag("id","app")));
            modulesService.updateTags(modulesTagsDto,mock(UserDetail.class));
            verify(modulesRepository,times(1)).updateFirst(any(),any(),any());
        }

        @Test
        void test_tagsIsNull(){
            ModulesTagsDto modulesTagsDto = new ModulesTagsDto();
            modulesTagsDto.setModuleId("test");
            Assertions.assertThrows(BizException.class,()->modulesService.updateTags(modulesTagsDto,mock(UserDetail.class)));
        }

    }
}
