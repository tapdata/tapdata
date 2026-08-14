package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import com.tapdata.tm.sso.dto.ImportRowResult;
import com.tapdata.tm.user.entity.User;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link SamlUserImportService}: reads an .xlsx workbook (POI), validates each
 * row, and either previews (dry-run) or applies the import. New users are created via
 * {@link SamlProvisioningService}; existing users are skipped or have their role
 * mappings upserted (UPDATE mode). Rows absent from the file are never touched.
 */
@Service
public class SamlUserImportServiceImpl implements SamlUserImportService {

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private final DataFormatter dataFormatter = new DataFormatter();

    @Autowired
    private SamlProvisioningService samlProvisioningService;

    @Autowired
    private RoleMappingService roleMappingService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public byte[] buildTemplate() {
        try (Workbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet(TEMPLATE_SHEET);
            Row header = sheet.createRow(0);
            for (int i = 0; i < TEMPLATE_HEADERS.length; i++) {
                header.createCell(i).setCellValue(TEMPLATE_HEADERS[i]);
            }
            Row example = sheet.createRow(1);
            example.createCell(0).setCellValue("jane.doe@example.com");
            example.createCell(1).setCellValue("jane.doe");
            example.createCell(2).setCellValue("Jane Doe");
            example.createCell(3).setCellValue("Analyst,Engineer");
            workbook.write(out);
            return out.toByteArray();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build user import template", e);
        }
    }

    @Override
    public ImportPreviewResult validate(MultipartFile file, ImportMode mode, UserDetail actor) {
        return process(file, mode, actor, true);
    }

    @Override
    public ImportPreviewResult confirm(MultipartFile file, ImportMode mode, UserDetail actor) {
        return process(file, mode, actor, false);
    }

    private ImportPreviewResult process(MultipartFile file, ImportMode mode, UserDetail actor, boolean dryRun) {
        if (mode == null) {
            mode = ImportMode.SKIP;
        }
        List<ImportRowResult> parsed = parse(file);
        Set<String> seen = new HashSet<>();
        ImportPreviewResult result = new ImportPreviewResult();
        result.setDryRun(dryRun);
        for (ImportRowResult row : parsed) {
            evaluateRow(row, seen, mode, actor, dryRun);
            result.getRows().add(row);
        }
        result.recomputeCounts();
        return result;
    }

    private void evaluateRow(ImportRowResult row, Set<String> seen, ImportMode mode, UserDetail actor, boolean dryRun) {
        String email = row.getEmail();
        if (StringUtils.isBlank(email)) {
            fail(row, "email is required");
            return;
        }
        email = email.trim().toLowerCase();
        row.setEmail(email);
        if (!EMAIL.matcher(email).matches()) {
            fail(row, "email is not a valid address");
            return;
        }
        if (!seen.add(email)) {
            fail(row, "duplicate email within the file");
            return;
        }

        User existing = findByEmail(email);
        if (existing == null) {
            row.setStatus(ImportRowResult.Status.CREATE);
            if (!dryRun) {
                samlProvisioningService.provisionUser(email, row.getUsername(), row.getRoleNames(), actor);
            }
            return;
        }
        if (mode == ImportMode.UPDATE) {
            row.setStatus(ImportRowResult.Status.UPDATE);
            if (!dryRun) {
                updateRoles(existing, row.getRoleNames(), actor);
            }
        } else {
            row.setStatus(ImportRowResult.Status.SKIP);
            row.setMessage("user already exists");
        }
    }

    private void updateRoles(User user, List<String> roleNames, UserDetail actor) {
        if (roleNames == null || roleNames.isEmpty()) {
            return;
        }
        String principalId = user.getId().toHexString();
        List<RoleMappingDto> mappings = new ArrayList<>();
        for (String roleName : roleNames) {
            if (StringUtils.isBlank(roleName)) {
                continue;
            }
            String roleId = samlProvisioningService.resolveOrCreateRoleId(roleName, actor);
            mappings.add(new RoleMappingDto("USER", principalId, new ObjectId(roleId)));
        }
        if (!mappings.isEmpty()) {
            roleMappingService.updateUserRoleMapping(mappings, actor);
        }
    }

    private void fail(ImportRowResult row, String message) {
        row.setStatus(ImportRowResult.Status.FAILED);
        row.setMessage(message);
    }

    private User findByEmail(String email) {
        Query query = Query.query(Criteria.where("email").is(email)
                .orOperator(Criteria.where("isDeleted").is(false), Criteria.where("isDeleted").exists(false)));
        return mongoTemplate.findOne(query, User.class);
    }

    private List<ImportRowResult> parse(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("import file is empty");
        }
        List<ImportRowResult> rows = new ArrayList<>();
        try (InputStream in = file.getInputStream();
             Workbook workbook = new XSSFWorkbook(in)) {
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet == null) {
                return rows;
            }
            int last = sheet.getLastRowNum();
            for (int r = 1; r <= last; r++) {
                Row row = sheet.getRow(r);
                if (row == null) {
                    continue;
                }
                String email = cell(row, 0);
                String username = cell(row, 1);
                String displayName = cell(row, 2);
                String roleNames = cell(row, 3);
                if (StringUtils.isAllBlank(email, username, displayName, roleNames)) {
                    continue;
                }
                ImportRowResult result = new ImportRowResult();
                result.setRow(r);
                result.setEmail(email);
                result.setUsername(username);
                result.setDisplayName(displayName);
                result.setRoleNames(splitRoles(roleNames));
                rows.add(result);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read import file: " + e.getMessage(), e);
        }
        return rows;
    }

    private List<String> splitRoles(String raw) {
        List<String> roles = new ArrayList<>();
        if (StringUtils.isNotBlank(raw)) {
            for (String part : raw.split(ROLE_DELIMITER)) {
                if (StringUtils.isNotBlank(part)) {
                    roles.add(part.trim());
                }
            }
        }
        return roles;
    }

    private String cell(Row row, int index) {
        Cell cell = row.getCell(index);
        if (cell == null) {
            return null;
        }
        String value = dataFormatter.formatCellValue(cell);
        return StringUtils.isBlank(value) ? null : value.trim();
    }
}
