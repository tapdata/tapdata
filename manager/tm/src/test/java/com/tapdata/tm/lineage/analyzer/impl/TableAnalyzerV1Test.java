package com.tapdata.tm.lineage.analyzer.impl;

import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.repository.ModulesRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableAnalyzerV1Test {
    @Nested
    class FindModulesTest{
        TableAnalyzerV1 tableAnalyzerV1;
        ModulesRepository modulesRepository;
        @BeforeEach
        void init(){
            modulesRepository = mock(ModulesRepository.class);
            tableAnalyzerV1 = new TableAnalyzerV1();
            ReflectionTestUtils.setField(tableAnalyzerV1,"modulesRepository",modulesRepository);
        }

        @Test
        void main_test(){
            List<ModulesEntity> modulesEntities = new ArrayList<>();
            ModulesEntity modules = new ModulesEntity();
            modulesEntities.add(modules);
            when(modulesRepository.findAll(any(Query.class))).thenAnswer(invocationOnMock -> {
                Query query = invocationOnMock.getArgument(0);
                Assertions.assertEquals(query.getQueryObject().get("datasource"),"test");
                Assertions.assertEquals(query.getQueryObject().get("tableName"),"test");
                System.out.println(query);
                return modulesEntities;
            });
            List<ModulesEntity> result = tableAnalyzerV1.findModules("test","test");
            Assertions.assertEquals(modulesEntities.size(),result.size());
        }
    }
}
