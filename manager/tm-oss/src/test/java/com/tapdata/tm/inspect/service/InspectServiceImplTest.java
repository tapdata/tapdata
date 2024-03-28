package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.inspect.repository.InspectRepository;
import com.tapdata.tm.utils.MessageUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class InspectServiceImplTest {
    @Nested
    class FieldHandler{
        @Test
        void test(){
            try(MockedStatic<MessageUtil> messageUtilMockedStatic = Mockito.mockStatic(MessageUtil.class)){
                messageUtilMockedStatic.when(()->MessageUtil.getMessage(anyString())).thenReturn("error");
                InspectServiceImpl inspectService = new InspectServiceImpl(mock(InspectRepository.class));
                Assertions.assertThrows(BizException.class,()->inspectService.fieldHandler(null,null));
            }
        }


    }
}
