package com.tapdata.tm.sso.controller;

import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.sso.dto.IdpMetadata;
import com.tapdata.tm.sso.dto.MetadataImportRequest;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlConfigView;
import com.tapdata.tm.sso.dto.SamlValidationResult;
import com.tapdata.tm.sso.dto.SpKeyPair;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import com.tapdata.tm.base.security.LoginUserResolver;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import com.tapdata.tm.sso.service.SamlConfigService;
import com.tapdata.tm.sso.service.SamlMetadataService;
import com.tapdata.tm.sso.service.SamlUserImportService;
import com.tapdata.tm.sso.service.SamlValidationService;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SsoConfigControllerTest {

    private SsoConfigController controller;

    @Mock
    private SamlConfigService samlConfigService;
    @Mock
    private SamlMetadataService samlMetadataService;
    @Mock
    private SamlValidationService samlValidationService;
    @Mock
    private SpKeyPairGenerator spKeyPairGenerator;
    @Mock
    private SamlUserImportService samlUserImportService;
    @Mock
    private LoginUserResolver loginUserResolver;

    @BeforeEach
    void setUp() {
        controller = new SsoConfigController();
        ReflectionTestUtils.setField(controller, "samlConfigService", samlConfigService);
        ReflectionTestUtils.setField(controller, "samlMetadataService", samlMetadataService);
        ReflectionTestUtils.setField(controller, "samlValidationService", samlValidationService);
        ReflectionTestUtils.setField(controller, "spKeyPairGenerator", spKeyPairGenerator);
        ReflectionTestUtils.setField(controller, "samlUserImportService", samlUserImportService);
        ReflectionTestUtils.setField(controller, "loginUserResolver", loginUserResolver);
    }

    @Test
    @DisplayName("getConfig returns the masked view")
    void getConfig() {
        SamlConfigView view = SamlConfigView.builder().enabled(true).build();
        when(samlConfigService.getMaskedConfig()).thenReturn(view);
        ResponseMessage<SamlConfigView> res = controller.getConfig();
        assertEquals(view, res.getData());
    }

    @Test
    @DisplayName("save delegates to service")
    void save() {
        SamlConfigForm form = new SamlConfigForm();
        controller.save(form);
        verify(samlConfigService).saveConfig(form);
    }

    @Test
    @DisplayName("validate delegates to validation service")
    void validate() {
        SamlConfigForm form = new SamlConfigForm();
        SamlValidationResult result = new SamlValidationResult();
        when(samlValidationService.validate(form)).thenReturn(result);
        assertEquals(result, controller.validate(form).getData());
    }

    @Test
    @DisplayName("import-idp-metadata parses XML")
    void importIdpMetadata() {
        MetadataImportRequest req = new MetadataImportRequest();
        req.setMetadataXml("<xml/>");
        IdpMetadata md = IdpMetadata.builder().idpEntityId("idp").build();
        when(samlMetadataService.parseIdpMetadata("<xml/>")).thenReturn(md);
        assertEquals(md, controller.importIdpMetadata(req).getData());
    }

    @Test
    @DisplayName("export-sp-metadata builds SP XML from current config")
    void exportSpMetadata() {
        SamlConfig config = SamlConfig.builder().spEntityId("sp").build();
        when(samlConfigService.getConfig()).thenReturn(config);
        when(samlMetadataService.buildSpMetadata(config)).thenReturn("<sp-metadata/>");
        assertEquals("<sp-metadata/>", controller.exportSpMetadata().getData());
    }

    @Test
    @DisplayName("generate-keypair stores the key and returns only the certificate (AC-053)")
    void generateKeyPair() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().spEntityId("https://sp").build());
        when(spKeyPairGenerator.generate(any()))
                .thenReturn(new SpKeyPair("PRIVATE-PEM", "CERT-PEM"));

        ResponseMessage<Map<String, String>> res = controller.generateKeyPair();

        // returned payload must contain the cert but never the private key
        assertEquals("CERT-PEM", res.getData().get("spCertificate"));
        assertNull(res.getData().get("spPrivateKey"));
        assertFalse(res.getData().values().stream().anyMatch(v -> v.contains("PRIVATE-PEM")));

        // the private key was passed to the service (which encrypts it at rest)
        ArgumentCaptor<SamlConfigForm> formCaptor = ArgumentCaptor.forClass(SamlConfigForm.class);
        verify(samlConfigService).saveConfig(formCaptor.capture());
        assertEquals("PRIVATE-PEM", formCaptor.getValue().getSpPrivateKey());
        assertEquals("CERT-PEM", formCaptor.getValue().getSpCertificate());
    }

    @Test
    @DisplayName("generate-keypair uses CN=spEntityId as subject DN")
    void generateKeyPairSubjectDn() {
        when(samlConfigService.getConfig()).thenReturn(SamlConfig.builder().spEntityId("https://sp").build());
        when(spKeyPairGenerator.generate(any())).thenReturn(new SpKeyPair("k", "c"));
        controller.generateKeyPair();
        verify(spKeyPairGenerator).generate("CN=https://sp");
    }

    @Test
    @DisplayName("user-import/template streams the workbook bytes as an attachment")
    void downloadImportTemplate() throws Exception {
        when(samlConfigService.isEnabled()).thenReturn(true);
        byte[] bytes = "XLSX".getBytes();
        when(samlUserImportService.buildTemplate()).thenReturn(bytes);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        when(response.getOutputStream()).thenReturn(new ServletOutputStream() {
            @Override public boolean isReady() { return true; }
            @Override public void setWriteListener(WriteListener l) { }
            @Override public void write(int b) { captured.write(b); }
        });

        controller.downloadImportTemplate(response);

        verify(response).setHeader(eq("Content-Disposition"),
                eq("attachment; filename=sso-user-import-template.xlsx"));
        assertEquals("XLSX", captured.toString());
    }

    @Test
    @DisplayName("user-import/validate parses mode and delegates with the login user")
    void validateImport() {
        when(samlConfigService.isEnabled()).thenReturn(true);
        MockMultipartFile file = new MockMultipartFile("file", new byte[]{1});
        UserDetail actor = mock(UserDetail.class);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
        when(loginUserResolver.resolve(any(), any())).thenReturn(actor);
        ImportPreviewResult preview = new ImportPreviewResult();
        when(samlUserImportService.validate(eq(file), eq(SamlUserImportService.ImportMode.UPDATE), eq(actor)))
                .thenReturn(preview);

        assertEquals(preview, controller.validateImport(file, "update").getData());
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    @DisplayName("unknown mode falls back to SKIP")
    void confirmImportUnknownModeFallsBackToSkip() {
        when(samlConfigService.isEnabled()).thenReturn(true);
        MockMultipartFile file = new MockMultipartFile("file", new byte[]{1});
        UserDetail actor = mock(UserDetail.class);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(new MockHttpServletRequest()));
        when(loginUserResolver.resolve(any(), any())).thenReturn(actor);
        ImportPreviewResult preview = new ImportPreviewResult();
        when(samlUserImportService.confirm(eq(file), eq(SamlUserImportService.ImportMode.SKIP), eq(actor)))
                .thenReturn(preview);

        assertEquals(preview, controller.confirmImport(file, "nonsense").getData());
        RequestContextHolder.resetRequestAttributes();
    }
}
