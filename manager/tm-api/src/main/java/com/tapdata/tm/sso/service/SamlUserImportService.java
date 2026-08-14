package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import org.springframework.web.multipart.MultipartFile;

/**
 * Admin-driven batch pre-creation of SSO users from an Excel (.xlsx) file. This is a
 * two-stage flow: {@link #validate} performs a dry-run and returns a per-row preview;
 * {@link #confirm} applies the changes. Only rows present in the file are touched -
 * users absent from the file are never disabled or deleted (BR-16), and SSO users are
 * created password-less (BR-17). This is not a continuous IdP sync (BR-14).
 */
public interface SamlUserImportService {

    /** How to handle a row whose email already maps to an existing user. */
    enum ImportMode {
        /** Leave the existing user untouched. */
        SKIP,
        /** Re-provision role assignments for the existing user. */
        UPDATE
    }

    /** Expected header, in column order. */
    String[] TEMPLATE_HEADERS = {"email", "username", "displayName", "roleNames"};

    /** Sheet name used by the generated template. */
    String TEMPLATE_SHEET = "users";

    /** Delimiter separating multiple role names within the roleNames cell. */
    String ROLE_DELIMITER = ",";

    /**
     * Build a downloadable .xlsx template (header row + one example row) for admins.
     *
     * @return the workbook bytes
     */
    byte[] buildTemplate();

    /**
     * Parse and validate the uploaded file without writing anything (dry-run). Each row
     * is checked for a well-formed, non-duplicate email; the resulting preview reports
     * per-row whether it would create/update/skip/fail.
     *
     * @param file  the uploaded .xlsx file
     * @param mode  how existing users would be handled
     * @param actor the acting admin
     * @return the dry-run preview
     */
    ImportPreviewResult validate(MultipartFile file, ImportMode mode, UserDetail actor);

    /**
     * Parse the uploaded file and apply it: new users are created, existing users are
     * skipped or updated per {@code mode}. Malformed rows are reported as failed and
     * skipped without aborting the whole import.
     *
     * @param file  the uploaded .xlsx file
     * @param mode  how existing users are handled
     * @param actor the acting admin
     * @return the applied result
     */
    ImportPreviewResult confirm(MultipartFile file, ImportMode mode, UserDetail actor);
}
