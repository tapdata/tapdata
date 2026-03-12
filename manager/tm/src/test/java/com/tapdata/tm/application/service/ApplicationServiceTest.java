package com.tapdata.tm.application.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
            verify(repository, times(1)).update(any(Query.class), any(ApplicationEntity.class));
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
            verify(repository, times(1)).updateByWhere(any(Query.class), any(), any(UserDetail.class));
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
    }
}