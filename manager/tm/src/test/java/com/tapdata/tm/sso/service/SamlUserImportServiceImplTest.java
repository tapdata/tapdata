package com.tapdata.tm.sso.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.sso.dto.ImportPreviewResult;
import com.tapdata.tm.sso.dto.ImportRowResult;
import com.tapdata.tm.sso.service.SamlUserImportService.ImportMode;
import com.tapdata.tm.user.entity.User;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SamlUserImportServiceImplTest {

    private SamlUserImportServiceImpl service;

    @Mock
    private SamlProvisioningService samlProvisioningService;
    @Mock
    private RoleMappingService roleMappingService;
    @Mock
    private MongoTemplate mongoTemplate;

    private final UserDetail actor = org.mockito.Mockito.mock(UserDetail.class);

    @BeforeEach
    void setUp() {
        service = new SamlUserImportServiceImpl();
        ReflectionTestUtils.setField(service, "samlProvisioningService", samlProvisioningService);
        ReflectionTestUtils.setField(service, "roleMappingService", roleMappingService);
        ReflectionTestUtils.setField(service, "mongoTemplate", mongoTemplate);
    }

    private MultipartFile xlsx(String[][] dataRows) throws Exception {
        try (Workbook wb = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = wb.createSheet("users");
            Row header = sheet.createRow(0);
            String[] headers = {"email", "username", "displayName", "roleNames"};
            for (int i = 0; i < headers.length; i++) {
                header.createCell(i).setCellValue(headers[i]);
            }
            for (int r = 0; r < dataRows.length; r++) {
                Row row = sheet.createRow(r + 1);
                for (int c = 0; c < dataRows[r].length; c++) {
                    if (dataRows[r][c] != null) {
                        row.createCell(c).setCellValue(dataRows[r][c]);
                    }
                }
            }
            wb.write(out);
            return new MockMultipartFile("file", "u.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", out.toByteArray());
        }
    }

    @Test
    @DisplayName("template is a readable xlsx with the expected header row")
    void buildTemplate() throws Exception {
        byte[] bytes = service.buildTemplate();
        assertTrue(bytes.length > 0);
        try (Workbook wb = new XSSFWorkbook(new ByteArrayInputStream(bytes))) {
            Row header = wb.getSheetAt(0).getRow(0);
            assertEquals("email", header.getCell(0).getStringCellValue());
            assertEquals("roleNames", header.getCell(3).getStringCellValue());
        }
    }

    @Test
    @DisplayName("validate dry-run: reports create/skip/failed per row and performs no writes")
    void validateDryRun() throws Exception {
        MultipartFile file = xlsx(new String[][]{
                {"new@corp.com", "new", "New User", "Analyst"},
                {"exists@corp.com", "e", "E", ""},
                {"bad-email", "b", "B", ""},
                {"new@corp.com", "dup", "Dup", ""}
        });
        when(mongoTemplate.findOne(any(Query.class), eq(User.class)))
                .thenAnswer(inv -> inv.getArgument(0).toString().contains("exists@corp.com") ? new User() : null);

        ImportPreviewResult result = service.validate(file, ImportMode.SKIP, actor);

        assertTrue(result.isDryRun());
        assertEquals(4, result.getTotal());
        assertEquals(1, result.getCreateCount());
        assertEquals(1, result.getSkipCount());
        assertEquals(2, result.getFailedCount());
        verify(samlProvisioningService, never()).provisionUser(any(), any(), anyList(), any());
    }

    @Test
    @DisplayName("confirm skip mode: creates new users, leaves existing untouched")
    void confirmSkip() throws Exception {
        MultipartFile file = xlsx(new String[][]{
                {"new@corp.com", "new", "New", "Analyst"},
                {"exists@corp.com", "e", "E", "Analyst"}
        });
        when(mongoTemplate.findOne(any(Query.class), eq(User.class)))
                .thenAnswer(inv -> inv.getArgument(0).toString().contains("exists@corp.com") ? new User() : null);

        ImportPreviewResult result = service.confirm(file, ImportMode.SKIP, actor);

        assertEquals(1, result.getCreateCount());
        assertEquals(1, result.getSkipCount());
        verify(samlProvisioningService, times(1))
                .provisionUser(eq("new@corp.com"), eq("new"), eq(List.of("Analyst")), eq(actor));
        verify(roleMappingService, never()).updateUserRoleMapping(anyList(), any());
    }

    @Test
    @DisplayName("confirm update mode: existing user gets role mappings upserted")
    void confirmUpdate() throws Exception {
        MultipartFile file = xlsx(new String[][]{{"exists@corp.com", "e", "E", "Analyst,Engineer"}});
        User existing = new User();
        existing.setId(new ObjectId());
        when(mongoTemplate.findOne(any(Query.class), eq(User.class))).thenReturn(existing);
        when(samlProvisioningService.resolveOrCreateRoleId(eq("Analyst"), any()))
                .thenReturn(new ObjectId().toHexString());
        when(samlProvisioningService.resolveOrCreateRoleId(eq("Engineer"), any()))
                .thenReturn(new ObjectId().toHexString());

        ImportPreviewResult result = service.confirm(file, ImportMode.UPDATE, actor);

        assertEquals(1, result.getUpdateCount());
        verify(roleMappingService).updateUserRoleMapping(anyList(), eq(actor));
    }

    @Test
    @DisplayName("first failed row is reported with a message")
    void failedRowHasMessage() throws Exception {
        MultipartFile file = xlsx(new String[][]{{"", "x", "X", ""}});
        ImportPreviewResult result = service.validate(file, ImportMode.SKIP, actor);
        ImportRowResult row = result.getRows().get(0);
        assertEquals(ImportRowResult.Status.FAILED, row.getStatus());
        assertTrue(row.getMessage() != null && !row.getMessage().isEmpty());
    }
}
