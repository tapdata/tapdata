package com.tapdata.tm.connectorRecord.service;

import com.tapdata.tm.commons.metrics.ConnectorRecordDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.connectorRecord.entity.ConnectorRecordEntity;
import com.tapdata.tm.connectorRecord.repository.ConnectorRecordRepository;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectorRecordServiceTest {

    @Mock
    private ConnectorRecordRepository mockRepository;
    @Mock
    private MongoTemplate mockMongoTemplate;

    private ConnectorRecordService connectorRecordServiceUnderTest;

    @BeforeEach
    void setUp() {
        connectorRecordServiceUnderTest = new ConnectorRecordService(mockRepository);
        ReflectionTestUtils.setField(connectorRecordServiceUnderTest, "mongoTemplate", mockMongoTemplate);
    }


    @Test
    void testUploadConnectorRecord() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final ConnectorRecordEntity expectedResult = new ConnectorRecordEntity();
        expectedResult.setPdkHash("pdkHash");
        expectedResult.setPdkId("pdkId");
        expectedResult.setStatus("status");
        expectedResult.setDownloadSpeed("downloadSpeed");
        expectedResult.setDownFiledMessage("downFiledMessage");

        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes"))
                .thenReturn(new Document("pdkId", "pdkId"));

        when(mockRepository.insert(expectedResult,userDetail)).thenReturn(expectedResult);

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testUploadConnectorRecord_MongoTemplateReturnsNull() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes")).thenReturn(null);

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isNull();
    }

    @Test
    void testUploadConnectorRecord_MongoTemplateReturnPdkIdsNull() {
        // Setup
        final ConnectorRecordDto connectorRecordDto = new ConnectorRecordDto();
        connectorRecordDto.setPdkHash("pdkHash");
        connectorRecordDto.setStatus("status");
        connectorRecordDto.setDownloadSpeed("downloadSpeed");
        connectorRecordDto.setDownFiledMessage("downFiledMessage");

        final UserDetail userDetail = new UserDetail("userId", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        when(mockMongoTemplate.findOne(Query.query(Criteria.where("pdkHash").is(connectorRecordDto.getPdkHash())), Document.class, "DatabaseTypes"))
                .thenReturn(new Document("pdkId", null));

        // Run the test
        final ConnectorRecordEntity result = connectorRecordServiceUnderTest.uploadConnectorRecord(connectorRecordDto,
                userDetail);

        // Verify the results
        assertThat(result).isNull();
    }

}
