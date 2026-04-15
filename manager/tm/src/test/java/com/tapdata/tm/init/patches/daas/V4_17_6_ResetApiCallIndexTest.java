package com.tapdata.tm.init.patches.daas;

import com.mongodb.client.MongoCollection;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.utils.HttpUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class V4_17_6_ResetApiCallIndexTest {

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private MongoCollection<Document> mongoCollection;

    private V4_17_6_ResetApiCallIndex patch;

    @BeforeEach
    void setUp() {
        patch = new V4_17_6_ResetApiCallIndex(null, null);
    }

    @Test
    void run_dropIndexSucceeded() {
        try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
             MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
            springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            mongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallEntity.class)).thenReturn("ApiCall");
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);

            assertDoesNotThrow(patch::run);

            verify(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");
        }
    }

    @Test
    void run_dropIndexThrowsShouldBeIgnored() {
        try (MockedStatic<SpringContextHelper> springContextHelper = mockStatic(SpringContextHelper.class);
             MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
            springContextHelper.when(() -> SpringContextHelper.getBean(MongoTemplate.class)).thenReturn(mongoTemplate);
            mongoUtils.when(() -> MongoUtils.getCollectionName(ApiCallEntity.class)).thenReturn("ApiCall");
            when(mongoTemplate.getCollection("ApiCall")).thenReturn(mongoCollection);
            doThrow(new RuntimeException("not exists")).when(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");

            assertDoesNotThrow(patch::run);

            verify(mongoCollection).dropIndex("createTime_1_hasMetric_1_delete_1");
        }
    }

    @Test
    void test() {
        String api = "http://127.0.0.1:3080/api/v1/a0swdcjt68x?access_token="
                + "eyJraWQiOiJhYjg2NTgyOS1kNjJmLTRiOGEtYTVkZS1mMWUxM2E5NWM2YWIiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbHVzdGVyIjoiNjk2MGJkZmM5YjhhODM1MDU0OWFjY2NiIiwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxNzc3MzczMjM0MDQwLCJhdWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjcmVhdGVkQXQiOjE3NzYxNjM2MzQwNDAsIm5iZiI6MTc3NjE2MzYzNCwiZXhwIjoxNzc3MzczMjM0LCJpYXQiOjE3NzYxNjM2MzQsImp0aSI6ImE2ZDZjNjFkLWRiYTEtNDJkOC04MzdhLWM2ZDQ0YTBmNjZmNSJ9.lnuXQm3V6CCK1cSs1NkqteMtrRQ4CExeNSecIwAzBDLEtIHd67zJlpGENziSVhVtBiumheKMY-iNgZiNnTLZV980xSnEZn4kDqLDu4nuIjdtScD3zFEctB81HveZXiE2P7L4AMKyG08qxQLDk-Ck52qtGqfb8TbuDVKX-7-PkuCApyVgcyVTCgUwNaBDOH_KL7N7Ux2x1JJoOGPtM3jhxSMeWizMlUqcDhxAcDoGEGM4diC6FFGKEqvE8k7IccUEG28_hG1lvy8HmNTxQn6HwDFCs9sCHb8tsxVvOr4hlk6aY4ruqGp5DkvpR0U7ySRX4GtyRiYbbXpngibcfKtAUA";
        AtomicInteger a = new AtomicInteger(0);
        for (int j = 0; j < 500; j++) {
            new Thread(() -> {
                for (int i = 0; i < 10000; i++) {
                    HttpUtils.sendGetData(api, new HashMap<>());
                }
                synchronized (a) {
                    a.addAndGet(1);
                }
            }).start();
        }

        while (a.get() < 500) {
            try {
                Thread.sleep(1000L);
            } catch (Exception e) {

            }
        }
    }
}
