package com.tapdata.tm.application.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;
import java.util.Optional;
import com.mongodb.client.MongoClient;
import com.tapdata.tm.base.reporitory.BaseRepository;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.List;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class ApplicationServiceTest {
    ApplicationService service;
    ApplicationRepository repository;
    UserDetail userDetail;

    @BeforeEach
    void init() {
        repository = mock(ApplicationRepository.class);
        service = new ApplicationService(repository);
        userDetail = mock(UserDetail.class);
    }

    @Nested
    class DataPermissionFindByIdTest {
        @Test
        void testFillFieldsAndDelegateFindById() {
            ApplicationService spyService = spy(service);
            ObjectId applicationId = new ObjectId();
            Field field = new Field();
            ApplicationDto applicationDto = new ApplicationDto();
            doReturn(applicationDto).when(spyService).findById(eq(applicationId), same(field));

            assertSame(applicationDto, spyService.dataPermissionFindById(applicationId, field).get());
            assertEquals(Boolean.TRUE, field.get(ApplicationService.USER_ID));
            assertEquals(Boolean.TRUE, field.get(DataPermissionHelper.FIELD_NAME));
            verify(spyService).findById(eq(applicationId), same(field));
        }
    }

    @Nested
    class countOfClientIdTest {
        @Test
        void testClientIdIsNull() {
            long result = service.countOfClientId(null, null);
            assertEquals(0, result);
            verify(repository, never()).count(any(Query.class));
        }
        @Test
        void testClientIdIsBlank() {
            long result = service.countOfClientId("   ", null);
            assertEquals(0, result);
            verify(repository, never()).count(any(Query.class));
        }
        @Test
        void testClientIdWithoutRecordId() {
            when(repository.count(any(Query.class))).thenReturn(5L);

            long result = service.countOfClientId("test-client-id", null);

            assertEquals(5, result);
            verify(repository, times(1)).count(any(Query.class));
        }
        @Test
        void testClientIdWithRecordId() {
            ObjectId recordId = new ObjectId();
            when(repository.count(any(Query.class))).thenReturn(3L);

            long result = service.countOfClientId("test-client-id", recordId);

            assertEquals(3, result);
            verify(repository, times(1)).count(any(Query.class));
        }
        @Test
        void testClientIdWithWhitespace() {
            when(repository.count(any(Query.class))).thenReturn(2L);

            long result = service.countOfClientId("  test-client-id  ", null);

            assertEquals(2, result);
            verify(repository, times(1)).count(any(Query.class));
        }
    }

    @Nested
    class updateByIdTest {
        ApplicationDto applicationDto;
        ObjectId id;

        @BeforeEach
        void setup() {
            id = new ObjectId();
            applicationDto = new ApplicationDto();
            applicationDto.setId(id);
            when(repository.findById(eq(id), any(Field.class)))
                    .thenAnswer(invocation -> Optional.of(persistedApplication()));
            when(repository.findById(eq(id), same(userDetail)))
                    .thenAnswer(invocation -> Optional.of(persistedApplication()));
        }

        private ApplicationEntity persistedApplication() {
            ApplicationEntity entity = new ApplicationEntity();
            entity.setId(id);
            entity.setClientId(applicationDto.getClientId());
            entity.setClientSecret("retained-secret");
            return entity;
        }

        @Test
        void testUpdateByIdWithDuplicateClientId() {
            applicationDto.setClientId("duplicate-client-id");
            when(repository.count(any(Query.class))).thenReturn(1L);

            BizException exception = assertThrows(BizException.class, () -> {
                service.updateById(applicationDto, userDetail);
            });

            assertEquals("api.server.client.id.exists", exception.getErrorCode());
        }

        @Test
        void testUpdateByIdWithEmptyClientId() {
            applicationDto.setClientId("");

            BizException exception = assertThrows(BizException.class, () -> {
                service.updateById(applicationDto, userDetail);
            });

            assertEquals("api.server.client.id.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateByIdWithBlankClientId() {
            applicationDto.setClientId("   ");

            BizException exception = assertThrows(BizException.class, () -> {
                service.updateById(applicationDto, userDetail);
            });

            assertEquals("api.server.client.id.empty", exception.getErrorCode());
        }

        @Test
        void testUpdateByIdAsRootUser() {
            applicationDto.setClientId("valid-client-id");
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(userDetail.isRoot()).thenReturn(true);
            when(userDetail.isFreeAuth()).thenReturn(false);

            ApplicationDto result = service.updateById(applicationDto, userDetail);

            assertNotNull(result);
            assertEquals(id, result.getId());
            assertTrue(result.isClientSecretConfigured());
            verify(repository, times(1)).update(any(Query.class), any(ApplicationEntity.class));
            verify(repository).findById(eq(id), any(Field.class));
        }

        @Test
        void testUpdateByIdAsNonRootUser() {
            applicationDto.setClientId("valid-client-id");
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(userDetail.isRoot()).thenReturn(false);
            when(repository.updateByWhere(any(Query.class), any(), any(UserDetail.class))).thenReturn(new UpdateResult(){

                @Override
                public boolean wasAcknowledged() {
                    return false;
                }

                @Override
                public long getMatchedCount() {
                    return 0;
                }

                @Override
                public long getModifiedCount() {
                    return 0;
                }

                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            });
            ApplicationDto result = service.updateById(applicationDto, userDetail);

            assertNotNull(result);
            assertEquals(id, result.getId());
            assertTrue(result.isClientSecretConfigured());
            verify(repository, times(1)).updateByWhere(any(Query.class), any(), any(UserDetail.class));
            verify(repository).findById(id, userDetail);
        }

        @Test
        void testUpdateByIdWithRootAndFreeAuth() {
            applicationDto.setClientId("valid-client-id");
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(userDetail.isRoot()).thenReturn(true);
            when(userDetail.isFreeAuth()).thenReturn(true);
            when(repository.updateByWhere(any(Query.class), any(), any(UserDetail.class))).thenReturn(new UpdateResult(){

                @Override
                public boolean wasAcknowledged() {
                    return false;
                }

                @Override
                public long getMatchedCount() {
                    return 0;
                }

                @Override
                public long getModifiedCount() {
                    return 0;
                }

                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            });
            ApplicationDto result = service.updateById(applicationDto, userDetail);

            assertNotNull(result);
            verify(repository, times(1)).updateByWhere(any(Query.class), any(), any(UserDetail.class));
        }

        @Test
        void testUpdateByIdWithNullClientId() {
            applicationDto.setClientId(null);
            when(repository.updateByWhere(any(Query.class), any(), any(UserDetail.class))).thenReturn(new UpdateResult(){

                @Override
                public boolean wasAcknowledged() {
                    return false;
                }

                @Override
                public long getMatchedCount() {
                    return 0;
                }

                @Override
                public long getModifiedCount() {
                    return 0;
                }

                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            });
            ApplicationDto result = service.updateById(applicationDto, userDetail);

            assertNotNull(result);
            verify(repository, never()).count(any(Query.class));
        }

        @Test
        void testUpdateByIdWithValidClientIdNoConflict() {
            applicationDto.setClientId("unique-client-id");
            when(repository.count(any(Query.class))).thenReturn(0L);
            when(repository.updateByWhere(any(Query.class), any(), any(UserDetail.class))).thenReturn(new UpdateResult(){

                @Override
                public boolean wasAcknowledged() {
                    return false;
                }

                @Override
                public long getMatchedCount() {
                    return 0;
                }

                @Override
                public long getModifiedCount() {
                    return 0;
                }

                @Override
                public BsonValue getUpsertedId() {
                    return null;
                }
            });
            when(userDetail.isRoot()).thenReturn(false);

            ApplicationDto result = service.updateById(applicationDto, userDetail);

            assertNotNull(result);
            assertEquals("unique-client-id", result.getClientId());
        }

        @Test
        void testUpdateByIdReturnsNullWhenRecordIsNotVisible() {
            when(repository.findById(id, userDetail)).thenReturn(Optional.empty());
            when(repository.updateByWhere(any(Query.class), any(), same(userDetail)))
                    .thenReturn(UpdateResult.acknowledged(0, 0L, null));

            assertNull(service.updateById(applicationDto, userDetail));
        }
    }

    @Nested
    class NonAdminUpdateByIdMappingTest {
        private Object previousFactory;

        @BeforeEach
        void saveFactory() {
            previousFactory = ReflectionTestUtils.getField(BaseRepository.class, "repositoryFactory");
            ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", null);
        }

        @AfterEach
        void restoreFactory() {
            ReflectionTestUtils.setField(BaseRepository.class, "repositoryFactory", previousFactory);
        }

        @Test
        void nonAdmin_updateById_targets_only_this_application() {
            MongoTemplate mongo = spy(new MongoTemplate(new SimpleMongoClientDatabaseFactory(mock(MongoClient.class), "test")));
            ApplicationService realService = new ApplicationService(new ApplicationRepository(mongo));
            ObjectId id = new ObjectId();
            ApplicationDto dto = new ApplicationDto();
            dto.setId(id);
            dto.setClientId(id.toHexString());
            UserDetail nonAdmin = new UserDetail("user-a", "tenant-a", "new-user", "", List.of());
            nonAdmin.setAuth();
            doReturn(0L).when(mongo).count(any(Query.class), eq(ApplicationEntity.class), anyString());
            doReturn(UpdateResult.acknowledged(1, 1L, null)).when(mongo)
                    .updateMulti(any(Query.class), any(UpdateDefinition.class), eq(ApplicationEntity.class));
            doReturn(null).when(mongo).findOne(any(Query.class), eq(ApplicationEntity.class), anyString());

            try (MockedStatic<DataPermissionHelper> ignored = mockStatic(DataPermissionHelper.class)) {
                realService.updateById(dto, nonAdmin);
            }

            ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
            verify(mongo).updateMulti(captor.capture(), any(UpdateDefinition.class), eq(ApplicationEntity.class));
            Document mapped = new QueryMapper(mongo.getConverter()).getMappedObject(captor.getValue().getQueryObject(),
                    mongo.getConverter().getMappingContext().getPersistentEntity(ApplicationEntity.class));
            assertEquals(id, mapped.get("_id"));
            assertEquals("tenant-a", mapped.get("customId"));
            assertEquals(List.of(new Document("user_id", "user-a"), new Document("user_id", new Document("$exists", false))),
                    mapped.getList("$or", Document.class));
        }
    }
}
