package com.tapdata.tm.sso.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of validating the SAML configuration before it is enabled.
 * <p>
 * {@link #valid} is {@code true} only when there are no errors. Warnings do not
 * make the configuration invalid but should be surfaced to the administrator.
 */
@Data
public class SamlValidationResult {

    private boolean valid;
    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> details = new ArrayList<>();

    public void addError(String message) {
        errors.add(message);
    }

    public void addWarning(String message) {
        warnings.add(message);
    }

    public void addDetail(String message) {
        details.add(message);
    }

    /** Recompute {@link #valid} from the current error list. */
    public SamlValidationResult finish() {
        this.valid = errors.isEmpty();
        return this;
    }
}
