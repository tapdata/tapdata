package com.tapdata.tm.modules.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.dto.ModulesDto;
import com.tapdata.tm.modules.dto.ModulesPermissionsDto;
import com.tapdata.tm.modules.dto.ModulesTagsDto;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.repository.ModulesRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    @Nested
    class BeforeSaveTest{
        @Test
        void test_main(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            fields.add(new Field());
            fields.add(null);
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(1,modules.getPaths().get(0).getFields().size());
        }
        @Test
        void test_paths_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().size());
        }
        @Test
        void test_paths_fields_isNull(){
            ModulesDto modules = new ModulesDto();
            List<Path> paths = new ArrayList<>();
            Path path = new Path();
            List<Field> fields = new ArrayList<>();
            path.setFields(fields);
            paths.add(path);
            modules.setPaths(paths);
            modulesService.beforeSave(modules,mock(UserDetail.class));
            Assertions.assertEquals(0,modules.getPaths().get(0).getFields().size());
        }
    }
}
