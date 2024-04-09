package com.tapdata.tm.agent.repository;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class AgentGroupRepositoryTest {
    AgentGroupRepository agentGroupRepository;
    @BeforeEach
    void init() {
        agentGroupRepository = mock(AgentGroupRepository.class);
    }
    @Nested
    class ConstructorTest {
        MongoRepositoryFactory repositoryFactory;
        MongoTemplate mongoOperations;
        @BeforeEach
        void init() {
            mongoOperations = mock(MongoTemplate.class);
            repositoryFactory = mock(MongoRepositoryFactory.class);
            when(repositoryFactory.getEntityInformation(any(Class.class))).thenReturn(mock(MongoEntityInformation.class));
        }

        @Test
        void testMongoRepositoryFactory() {
            try (MockedConstruction<MongoRepositoryFactory> mrf = mockConstruction(MongoRepositoryFactory.class)) {
                Assertions.assertDoesNotThrow(() -> new AgentGroupRepository(mongoOperations));
            }
        }
    }

    @Nested
    class InitTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(agentGroupRepository).init();
            Assertions.assertDoesNotThrow(() -> agentGroupRepository.init());
        }
    }
}