package com.tapdata.tm.sso.service;

import com.tapdata.tm.accessToken.service.AccessTokenService;
import com.tapdata.tm.sso.entity.SsoSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlSessionServiceImplTest {

    private SamlSessionServiceImpl service;

    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private AccessTokenService accessTokenService;

    @BeforeEach
    void setUp() {
        service = new SamlSessionServiceImpl();
        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
        ReflectionTestUtils.setField(service, "accessTokenService", accessTokenService);
    }

    private SsoSession session(String tokenId, String nameId, String sessionIndex) {
        SsoSession s = new SsoSession();
        s.setAccessTokenId(tokenId);
        s.setNameId(nameId);
        s.setSessionIndex(sessionIndex);
        return s;
    }

    @Test
    @DisplayName("terminateByAccessToken revokes the token and deletes the session")
    void terminateByAccessToken() {
        when(mongoTemplate.findOne(any(Query.class), eq(SsoSession.class)))
                .thenReturn(session("tok-1", "user@x", "idx-1"));
        when(accessTokenService.removeAccessToken(eq("tok-1"), isNull())).thenReturn(1L);

        assertEquals(1, service.terminateByAccessToken("tok-1"));
        verify(accessTokenService).removeAccessToken(eq("tok-1"), isNull());
        verify(mongoTemplate).remove(any(Query.class), eq(SsoSession.class));
    }

    @Test
    @DisplayName("terminateByAccessToken still revokes the token when no session record exists")
    void terminateByAccessTokenNoSession() {
        when(mongoTemplate.findOne(any(Query.class), eq(SsoSession.class))).thenReturn(null);
        when(accessTokenService.removeAccessToken(eq("tok-1"), isNull())).thenReturn(1L);

        assertEquals(1, service.terminateByAccessToken("tok-1"));
        verify(accessTokenService).removeAccessToken(eq("tok-1"), isNull());
        verify(mongoTemplate, never()).remove(any(Query.class), eq(SsoSession.class));
    }

    @Test
    @DisplayName("terminateByAccessToken with blank id is a no-op")
    void terminateByAccessTokenBlank() {
        assertEquals(0, service.terminateByAccessToken("  "));
        verify(accessTokenService, never()).removeAccessToken(any(), any());
    }

    @Test
    @DisplayName("terminate by nameId+sessionIndex revokes each matching session's token")
    void terminateByNameIdAndSessionIndex() {
        when(mongoTemplate.find(any(Query.class), eq(SsoSession.class)))
                .thenReturn(Collections.singletonList(session("tok-1", "user@x", "idx-1")));

        assertEquals(1, service.terminate("user@x", "idx-1"));
        verify(accessTokenService).removeAccessToken(eq("tok-1"), isNull());
        verify(mongoTemplate).remove(any(Query.class), eq(SsoSession.class));
    }

    @Test
    @DisplayName("terminate by nameId only revokes all of the subject's sessions")
    void terminateByNameIdAll() {
        when(mongoTemplate.find(any(Query.class), eq(SsoSession.class)))
                .thenReturn(Arrays.asList(session("tok-1", "user@x", "a"), session("tok-2", "user@x", "b")));

        assertEquals(2, service.terminate("user@x", null));
        verify(accessTokenService).removeAccessToken(eq("tok-1"), isNull());
        verify(accessTokenService).removeAccessToken(eq("tok-2"), isNull());
    }

    @Test
    @DisplayName("terminate with blank nameId is a no-op")
    void terminateBlankNameId() {
        assertEquals(0, service.terminate("  ", "idx"));
        verify(mongoTemplate, never()).find(any(Query.class), eq(SsoSession.class));
    }

    @Test
    @DisplayName("isExpired: null session and past SessionNotOnOrAfter are expired; null/future are valid")
    void isExpired() {
        assertTrue(service.isExpired(null));

        SsoSession noExpiry = session("t", "n", "i");
        noExpiry.setSessionNotOnOrAfter(null);
        assertFalse(service.isExpired(noExpiry));

        SsoSession past = session("t", "n", "i");
        past.setSessionNotOnOrAfter(new Date(System.currentTimeMillis() - 60_000));
        assertTrue(service.isExpired(past));

        SsoSession future = session("t", "n", "i");
        future.setSessionNotOnOrAfter(new Date(System.currentTimeMillis() + 60_000));
        assertFalse(service.isExpired(future));
    }

    @Test
    @DisplayName("terminate filters by nameId (and sessionIndex when provided)")
    void terminateQueryFilters() {
        when(mongoTemplate.find(any(Query.class), eq(SsoSession.class)))
                .thenReturn(Collections.emptyList());
        service.terminate("user@x", "idx-1");
        ArgumentCaptor<Query> captor = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).find(captor.capture(), eq(SsoSession.class));
        String q = captor.getValue().getQueryObject().toJson();
        assertTrue(q.contains("nameId"));
        assertTrue(q.contains("sessionIndex"));
    }
}
