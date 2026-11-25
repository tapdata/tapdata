package com.tapdata.tm.proxyprocess.service.impl;

import com.tapdata.tm.proxyprocess.dto.AgentProcessRegisterDto;
import com.tapdata.tm.proxyprocess.dto.AgentProcessStatusDto;
import com.tapdata.tm.proxyprocess.entity.AgentProcessEntity;
import com.tapdata.tm.proxyprocess.repository.AgentProcessRepository;
import com.tapdata.tm.proxyprocess.service.auth.AgentAccessTokenIssuer;
import com.tapdata.tm.proxyprocess.service.auth.AgentCredentialService;
import com.tapdata.tm.proxyprocess.service.auth.AgentOAuth2ClientRegistrar;
import com.tapdata.tm.proxyprocess.service.auth.AgentSecretCipher;
import com.tapdata.tm.proxyprocess.service.auth.AgentTokenValidator;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.tapdata.tm.config.security.UserDetail;
import org.mockito.ArgumentCaptor;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/**
 * Covers the agent-owns-its-credential contract:
 * an agent generates its own OAuth2 client and reports it at register time, TM records it, and
 * the legacy one-token-proves-both-directions model stops applying to that agent.
 */
class AgentProcessCredentialTest {

    private static final String OWNER_ID = "6062c8fd764d49c7d626f7e3";


    private static final String CLIENT_ID = "550e8400e29b41d4a7164466";
    private static final String SECRET = "0123456789abcdef0123456789abcdef";

    private AgentProcessRepository repository;
    private AgentCredentialService credentials;
    /* The real chain over mocked collaborators: routing is exercised, crypto is not. */
    private AgentTokenValidator tokenValidator;
    private AgentOAuth2ClientRegistrar registrar;
    private AgentSecretCipher cipher;
    private AgentProcessServiceImpl service;

    @BeforeEach
    void setUp() {
        repository = mock(AgentProcessRepository.class);
        tokenValidator = mock(AgentTokenValidator.class);
        registrar = mock(AgentOAuth2ClientRegistrar.class);
        cipher = mock(AgentSecretCipher.class);
        when(cipher.hash(anyString())).thenAnswer(i -> "hash:" + i.getArgument(0));
        when(cipher.encrypt(anyString())).thenAnswer(i -> "enc:" + i.getArgument(0));
        when(cipher.matches(anyString(), anyString())).thenAnswer(i ->
                ("hash:" + i.getArgument(0)).equals(i.getArgument(1)));
        credentials = new AgentCredentialService(
                tokenValidator, registrar, cipher, mock(AgentAccessTokenIssuer.class));
        when(repository.saveAgentProcess(any(AgentProcessEntity.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        service = new AgentProcessServiceImpl(repository, credentials);
    }

    private AgentProcessRegisterDto request(String agentId, String clientId, String secret) {
        AgentProcessRegisterDto request = new AgentProcessRegisterDto();
        request.setAgentName("reader-host-1");
        request.setAgentType("Db2 for z cdc reader");
        request.setAgentId(agentId);
        request.setAdvertiseIp("10.0.0.10");
        request.setAdvertisePort(18080);
        request.setStartTime(new Date(1000));
        request.setClientId(clientId);
        request.setClientSecret(secret);
        return request;
    }

    @Test
    void registerStoresReportedCredentialsAndPublishesOAuth2Client() {
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.empty());
        
        

        service.register(request("agent-1", CLIENT_ID, SECRET), caller(OWNER_ID));

        ArgumentCaptor<AgentProcessEntity> saved = ArgumentCaptor.forClass(AgentProcessEntity.class);
        verify(repository, atLeastOnce()).saveAgentProcess(saved.capture());
        AgentProcessEntity entity = saved.getValue();
        assertEquals(CLIENT_ID, entity.getClientId());
        /*
         * Hash for change detection, ciphertext for reveal: never the raw secret.
         * The registration layer only routes to the credential chain.
         */
        assertEquals("hash:" + SECRET, entity.getClientSecretHash());
        assertEquals("enc:" + SECRET, entity.getClientSecretCipher());
        verify(registrar).register(eq(CLIENT_ID), eq(SECRET), anyList());
    }

    @Test
    void partialCredentialReportIsRejected() {
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.empty());

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> service.register(request("agent-1", CLIENT_ID, null), caller(OWNER_ID)));
        assertTrue(error.getMessage().contains("together"));
    }

    @Test
    void clientIdUsedByAnotherAgentIsRejected() {
        AgentProcessEntity existing = new AgentProcessEntity();
        existing.setAgentId("agent-1");
        existing.setClientId("aaaaaaaaaaaaaaaaaaaaaaaa");
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));
        /* Some other registration of this agent type already owns the client id. */
        when(repository.clientIdUsedByAnotherAgent("Db2 for z cdc reader", CLIENT_ID,
                "agent-1")).thenReturn(true);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> service.register(request("agent-1", CLIENT_ID, SECRET), caller(OWNER_ID)));
        assertTrue(error.getMessage().contains("another agent"));
    }

    /*
     * A Reader registers on every start with the client id it generated for itself. Treating
     * the same agent coming back with its own client id as a conflict would make every restart
     * fail, so the same agent at the same client id must pass - and keeps its credentials.
     */
    @Test
    void reRegisteringTheSameAgentWithItsOwnClientIdIsAccepted() {
        AgentProcessEntity existing = new AgentProcessEntity();
        existing.setAgentId("agent-1");
        existing.setClientId(CLIENT_ID);
        existing.setStartTime(1000L);
        existing.setClientSecretHash("hash:" + SECRET);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));
        when(repository.clientIdUsedByAnotherAgent("Db2 for z cdc reader", CLIENT_ID,
                "agent-1")).thenReturn(false);

        service.register(request("agent-1", CLIENT_ID, SECRET), caller(OWNER_ID));

        assertEquals(CLIENT_ID, existing.getClientId());
        verify(registrar, never()).register(anyString(), anyString(), anyList());
    }

    @Test
    void reRegisterWithUnchangedSecretDoesNotRepublishTheClient() {
        AgentProcessEntity existing = new AgentProcessEntity();
        existing.setAgentId("agent-1");
        existing.setClientId(CLIENT_ID);
        existing.setStartTime(1000L);
        /* Stored in the same shape the chain produces, so matches() sees no change. */
        existing.setClientSecretHash("hash:" + SECRET);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));

        service.register(request("agent-1", CLIENT_ID, SECRET), caller(OWNER_ID));

        verify(registrar, never()).register(anyString(), anyString(), anyList());
    }

    @Test
    void statusReportAuthenticatedByAccessCodeUsesTheOwningUser() {
        AgentProcessEntity existing = new AgentProcessEntity();
        existing.setAgentId("agent-1");
        existing.setClientId(CLIENT_ID);
        existing.setStartTime(1000L);
        existing.setUserId(OWNER_ID);
        when(repository.findByAgentId("agent-1")).thenReturn(Optional.of(existing));

        AgentProcessStatusDto status = new AgentProcessStatusDto();
        status.setAgentId("agent-1");
        status.setStartTime(new Date(1000));
        status.setPid(1L);
        status.setStatus("RUNNING");
        status.setStatusReportTime(new Date());

        service.reportStatus(status);

        assertEquals("RUNNING", existing.getStatus());
    }


    @Test
    void deletingAReaderRemovesItsOAuth2Client() {
        ObjectId id = new ObjectId();
        AgentProcessEntity existing = new AgentProcessEntity();
        existing.setId(id);
        existing.setAgentId("agent-1");
        existing.setClientId(CLIENT_ID);
        when(repository.findById(id)).thenReturn(Optional.of(existing));
        when(repository.deleteById(id)).thenReturn(true);

        service.delete(id.toHexString());

        verify(registrar).unregister(CLIENT_ID);
    }

    private static UserDetail caller(String userId) {
        return new UserDetail(userId, userId, "reader-caller", "", java.util.List.of());
    }
}
