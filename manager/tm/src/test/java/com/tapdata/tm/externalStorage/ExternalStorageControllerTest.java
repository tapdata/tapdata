package com.tapdata.tm.externalStorage;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageType;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.controller.ExternalStorageController;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

public class ExternalStorageControllerTest {

    @Test
    void saveShouldMaskMongoUriInResponse() {
        ExternalStorageController controller = prepareController();
        ExternalStorageService externalStorageService = (ExternalStorageService) ReflectionTestUtils.getField(controller, "externalStorageService");
        UserDetail userDetail = controller.getLoginUser();
        ExternalStorageDto saved = mongodbStorage("mongodb://admin:password@localhost:27017/test");
        when(externalStorageService.save(any(ExternalStorageDto.class), Mockito.eq(userDetail))).thenReturn(saved);

        try (MockedStatic<RequestContextHolder> holderMockedStatic = mockNonAgentRequest()) {
            ResponseMessage<ExternalStorageDto> response = controller.save(mongodbStorage("mongodb://admin:password@localhost:27017/test"));

            assertEquals("mongodb://admin:******@localhost:27017/test", response.getData().getUri());
        }
    }

    @Test
    void findByIdShouldMaskMongoUriWhenTypeIsNotReturned() {
        ExternalStorageController controller = prepareController();
        ExternalStorageService externalStorageService = (ExternalStorageService) ReflectionTestUtils.getField(controller, "externalStorageService");
        UserDetail userDetail = controller.getLoginUser();
        ExternalStorageDto storage = new ExternalStorageDto();
        storage.setUri("mongodb://admin:password@localhost:27017/test");
        ObjectId id = new ObjectId();
        when(externalStorageService.findById(Mockito.eq(id), any(), Mockito.eq(userDetail))).thenReturn(storage);

        try (MockedStatic<RequestContextHolder> holderMockedStatic = mockNonAgentRequest()) {
            ResponseMessage<ExternalStorageDto> response = controller.findById(id.toHexString(), null);

            assertEquals("mongodb://admin:******@localhost:27017/test", response.getData().getUri());
        }
    }

    private MockedStatic<RequestContextHolder> mockNonAgentRequest() {
        MockedStatic<RequestContextHolder> holderMockedStatic = Mockito.mockStatic(RequestContextHolder.class);
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest();
        ServletRequestAttributes servletRequestAttributes = new ServletRequestAttributes(mockHttpServletRequest);
        holderMockedStatic.when(RequestContextHolder::currentRequestAttributes).thenReturn(servletRequestAttributes);
        return holderMockedStatic;
    }

    private ExternalStorageController prepareController() {
        ExternalStorageController controller = Mockito.spy(new ExternalStorageController());
        ExternalStorageService externalStorageService = Mockito.mock(ExternalStorageService.class);
        SettingsService settingsService = Mockito.mock(SettingsService.class);
        PermissionService permissionService = Mockito.mock(PermissionService.class);
        UserDetail userDetail = Mockito.mock(UserDetail.class);
        doReturn(userDetail).when(controller).getLoginUser();
        when(settingsService.isCloud()).thenReturn(true);
        ReflectionTestUtils.setField(controller, "externalStorageService", externalStorageService);
        ReflectionTestUtils.setField(controller, "settingsService", settingsService);
        ReflectionTestUtils.setField(controller, "permissionService", permissionService);
        return controller;
    }

    @Test
    void saveShouldMaskSensitiveFieldsInResponse() {
        ExternalStorageController controller = prepareController();
        ExternalStorageService externalStorageService = (ExternalStorageService) ReflectionTestUtils.getField(controller, "externalStorageService");
        UserDetail userDetail = controller.getLoginUser();
        ExternalStorageDto saved = mongodbStorage("mongodb://admin:password@localhost:27017/test");
        saved.setSslCA("ca");
        saved.setSslKey("key");
        saved.setSslPass("pass");
        saved.setAccessToken("token");
        when(externalStorageService.save(any(ExternalStorageDto.class), Mockito.eq(userDetail))).thenReturn(saved);

        try (MockedStatic<RequestContextHolder> holderMockedStatic = mockNonAgentRequest()) {
            ResponseMessage<ExternalStorageDto> response = controller.save(mongodbStorage("mongodb://admin:password@localhost:27017/test"));

            assertEquals(ExternalStorageDto.MASK_PWD, response.getData().getSslCA());
            assertEquals(ExternalStorageDto.MASK_PWD, response.getData().getSslKey());
            assertEquals(ExternalStorageDto.MASK_PWD, response.getData().getSslPass());
            assertEquals(ExternalStorageDto.MASK_PWD, response.getData().getAccessToken());
        }
    }

    private ExternalStorageDto mongodbStorage(String uri) {
        ExternalStorageDto externalStorageDto = new ExternalStorageDto();
        externalStorageDto.setType(ExternalStorageType.mongodb.name());
        externalStorageDto.setUri(uri);
        return externalStorageDto;
    }
}
