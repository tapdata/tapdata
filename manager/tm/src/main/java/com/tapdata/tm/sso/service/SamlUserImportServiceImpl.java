package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import com.tapdata.tm.sso.dto.ImportRowResult;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.constant.Operation;
import com.tapdata.tm.userLog.service.UserLogService;
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
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
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

    @Autowired
    private UserLogService userLogService;

    @Override
    public byte[] buildTemplate() {
        try (Workbook workbook = new XSSFWorkbook();
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet(TEMPLATE_SHEET);
            Row header = sheet.createRow(0);
            for (int i = 0; i < TEMPLATE_HEADERS.length; i++) {
                header.createCell(i).setCellValue(TEMPLATE_HEADERS[i]);
            }
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
        if (!dryRun) {
            recordImportAudit(file, mode, result, actor);
        }
        return result;
    }

    private void evaluateRow(ImportRowResult row, Set<String> seen, ImportMode mode, UserDetail actor, boolean dryRun) {
        String email = row.getEmail();
        if (StringUtils.isBlank(email)) {
            fail(row, "email is required");
            return;
        }
        email = email.trim().toLowerCase(Locale.ROOT);
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
        try {
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
                    updateUser(existing, row.getUsername(), row.getRoleNames(), actor);
                }
            } else {
                row.setStatus(ImportRowResult.Status.SKIP);
                row.setMessage("user already exists");
            }
        } catch (Exception e) {
            // Isolate a bad row so earlier successful rows remain auditable and the
            // remaining file can still be processed.
            fail(row, StringUtils.defaultIfBlank(e.getMessage(), "row processing failed"));
        }
    }

    private void updateUser(User user, String username, List<String> roleNames, UserDetail actor) {
        String principalId = user.getId().toHexString();
        List<RoleMappingDto> mappings = new ArrayList<>();
        Set<ObjectId> desiredRoleIds = new HashSet<>();
        if (roleNames != null) {
            for (String roleName : roleNames) {
                if (StringUtils.isBlank(roleName)) {
                    continue;
                }
                String roleId = samlProvisioningService.resolveOrCreateRoleId(roleName, actor);
                ObjectId objectId = new ObjectId(roleId);
                desiredRoleIds.add(objectId);
                mappings.add(new RoleMappingDto(PrincipleType.USER.getValue(), principalId, objectId));
            }
        }
        // A non-empty roleNames cell is a replacement, not an additive upsert.
        // Keep existing assignments when the optional cell is blank. Update both the
        // normalized role-mapping collection and User.roleusers: different permission
        // readers use these two representations, and updating only one makes roles
        // appear to be cleared after an import.
        if (roleNames != null && !roleNames.isEmpty()) {
            roleMappingService.deleteAll(Query.query(Criteria.where("principalId").is(principalId)
                    .and("principalType").is(PrincipleType.USER.getValue())));
            if (!mappings.isEmpty()) {
                roleMappingService.updateUserRoleMapping(mappings, actor);
            }
        }
        org.springframework.data.mongodb.core.query.Update userUpdate = new org.springframework.data.mongodb.core.query.Update();
        boolean userChanged = false;
        if (StringUtils.isNotBlank(username) && !StringUtils.equals(username.trim(), user.getUsername())) {
            userUpdate.set("username", username.trim());
            userChanged = true;
        }
        if (roleNames != null && !roleNames.isEmpty()) {
            userUpdate.set("roleusers", desiredRoleIds.stream()
                    .map(ObjectId::toHexString)
                    .collect(Collectors.toList()));
            userChanged = true;
        }
        if (userChanged) {
            mongoTemplate.updateFirst(Query.query(Criteria.where("_id").is(user.getId())), userUpdate, User.class);
        }
        userLogService.addUserLog(Modular.USER, Operation.BATCH_UPDATE, actor,
                principalId, user.getEmail(), StringUtils.join(roleNames == null ? List.of() : roleNames, ","), false);
    }

    private void recordImportAudit(MultipartFile file, ImportMode mode, ImportPreviewResult result, UserDetail actor) {
        String fileName = StringUtils.defaultIfBlank(file == null ? null : file.getOriginalFilename(), "(unnamed)");
        String summary = String.format("file=%s, mode=%s, total=%d, created=%d, updated=%d, skipped=%d, failed=%d",
                fileName, mode, result.getTotal(), result.getCreateCount(), result.getUpdateCount(),
                result.getSkipCount(), result.getFailedCount());
        // UserLogService records the acting administrator and creation timestamp from
        // UserDetail/UserLogEntity; keep the filename and deterministic result summary
        // in the operation parameter for audit queries and exports.
        userLogService.addUserLog(Modular.USER, Operation.BATCH_UPDATE, actor, summary);
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
                String roleNames = cell(row, 2);
                if (StringUtils.isAllBlank(email, username, roleNames)) {
                    continue;
                }
                ImportRowResult result = new ImportRowResult();
                result.setRow(r);
                result.setEmail(email);
                result.setUsername(username);
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
