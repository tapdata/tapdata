package com.tapdata.tm.sso.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.sso.dto.IdpMetadata;
import com.tapdata.tm.sso.dto.MetadataImportRequest;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlConfigView;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import com.tapdata.tm.sso.dto.SamlValidationResult;
import com.tapdata.tm.sso.dto.SpKeyPair;
import com.tapdata.tm.sso.security.SpKeyPairGenerator;
import com.tapdata.tm.sso.service.SamlConfigService;
import com.tapdata.tm.sso.service.SamlMetadataService;
import com.tapdata.tm.sso.service.SamlUserImportService;
import com.tapdata.tm.sso.service.SamlValidationService;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Admin-facing REST API for the single, IdP-agnostic SAML SSO configuration:
 * read (masked), save, IdP metadata import, SP metadata export, SP key pair
 * generation and validation. The SP private key is never returned by any endpoint
 * (AC-053).
 */
@Slf4j
@RestController
@RequestMapping("/api/sso/saml/config")
@Setter(onMethod_ = {@Autowired})
public class SsoConfigController extends BaseController {

    private SamlConfigService samlConfigService;
    private SamlMetadataService samlMetadataService;
    private SamlValidationService samlValidationService;
    private SpKeyPairGenerator spKeyPairGenerator;
    private SamlUserImportService samlUserImportService;

    @Operation(summary = "Get the current SAML configuration (private key masked)")
    @GetMapping
    public ResponseMessage<SamlConfigView> getConfig() {
        return success(samlConfigService.getMaskedConfig());
    }

    @Operation(summary = "Save the SAML configuration")
    @PostMapping("/save")
    public ResponseMessage<Void> save(@RequestBody SamlConfigForm form) {
        samlConfigService.saveConfig(form);
        return success();
    }

    @Operation(summary = "Validate the SAML configuration")
    @PostMapping("/validate")
    public ResponseMessage<SamlValidationResult> validate(@RequestBody SamlConfigForm form) {
        return success(samlValidationService.validate(form));
    }

    @Operation(summary = "Parse an IdP metadata XML document to prefill IdP fields")
    @PostMapping("/import-idp-metadata")
    public ResponseMessage<IdpMetadata> importIdpMetadata(@RequestBody MetadataImportRequest request) {
        String xml = request == null ? null : request.getMetadataXml();
        return success(samlMetadataService.parseIdpMetadata(xml));
    }

    @Operation(summary = "Export the TapData SP metadata XML")
    @GetMapping("/export-sp-metadata")
    public ResponseMessage<String> exportSpMetadata() {
        SamlConfig config = samlConfigService.getConfig();
        return success(samlMetadataService.buildSpMetadata(config));
    }

    /**
     * Generate a new SP signing/decryption key pair. The private key is encrypted
     * and stored immediately; only the certificate is returned (AC-053).
     */
    @Operation(summary = "Generate and store a new SP key pair; returns only the certificate")
    @PostMapping("/generate-keypair")
    public ResponseMessage<Map<String, String>> generateKeyPair() {
        SamlConfig config = samlConfigService.getConfig();
        String subjectDn = StringUtils.isNotBlank(config.getSpEntityId())
                ? "CN=" + config.getSpEntityId() : "CN=TapData SAML SP";
        SpKeyPair keyPair = spKeyPairGenerator.generate(subjectDn);

        SamlConfigForm form = new SamlConfigForm();
        form.setSpPrivateKey(keyPair.getPrivateKeyPem());
        form.setSpCertificate(keyPair.getCertificatePem());
        samlConfigService.saveConfig(form);

        Map<String, String> result = new HashMap<>();
        result.put("spCertificate", keyPair.getCertificatePem());
        return success(result);
    }

    @Operation(summary = "Download the SSO batch user import template (.xlsx)")
    @GetMapping("/user-import/template")
    public void downloadImportTemplate(HttpServletResponse response) {
        byte[] bytes = samlUserImportService.buildTemplate();
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        response.setHeader("Content-Disposition", "attachment; filename=sso-user-import-template.xlsx");
        try (ServletOutputStream out = response.getOutputStream()) {
            out.write(bytes);
            out.flush();
        } catch (IOException e) {
            log.error("Failed to write user import template", e);
        }
    }

    @Operation(summary = "Validate an SSO batch user import file (dry-run, no writes)")
    @PostMapping(path = "/user-import/validate", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<ImportPreviewResult> validateImport(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "mode", required = false, defaultValue = "SKIP") String mode) {
        return success(samlUserImportService.validate(file, parseMode(mode), getLoginUser()));
    }

    @Operation(summary = "Apply an SSO batch user import file")
    @PostMapping(path = "/user-import/confirm", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<ImportPreviewResult> confirmImport(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "mode", required = false, defaultValue = "SKIP") String mode) {
        return success(samlUserImportService.confirm(file, parseMode(mode), getLoginUser()));
    }

    private SamlUserImportService.ImportMode parseMode(String mode) {
        if (StringUtils.isBlank(mode)) {
            return SamlUserImportService.ImportMode.SKIP;
        }
        try {
            return SamlUserImportService.ImportMode.valueOf(mode.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return SamlUserImportService.ImportMode.SKIP;
        }
    }
}
