package com.tapdata.tm.ds.service.impl;


import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataSourceServiceImplTest {
    DataSourceServiceImpl dataSourceService;
    @Nested
    class RestoreAccessNodeTypeTest{
        DataSourceConnectionDto updateDto;
        DataSourceConnectionDto connectionDto;
        List<String> accessNodeProcessIdList;
        @BeforeEach
        void init() {
            dataSourceService = mock(DataSourceServiceImpl.class);

            updateDto = mock(DataSourceConnectionDto.class);
            connectionDto = mock(DataSourceConnectionDto.class);
            accessNodeProcessIdList = new ArrayList<>();
            accessNodeProcessIdList.add("");

            when(connectionDto.getAccessNodeType()).thenReturn("");
            doNothing().when(updateDto).setAccessNodeType(anyString());
            doNothing().when(updateDto).setAccessNodeProcessId(anyString());
            doNothing().when(updateDto).setAccessNodeProcessIdList(accessNodeProcessIdList);
            doNothing().when(updateDto).setAccessNodeTypeEmpty(false);

            when(updateDto.isAccessNodeTypeEmpty()).thenReturn(true);

            doCallRealMethod().when(dataSourceService).restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList);
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(1)).getAccessNodeType();
            verify(updateDto, times(1)).setAccessNodeType(anyString());
            verify(updateDto, times(1)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(1)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(1)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }
        @Test
        void testNotIsAccessNodeTypeEmpty() {
            when(updateDto.isAccessNodeTypeEmpty()).thenReturn(false);
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(0)).getAccessNodeType();
            verify(updateDto, times(0)).setAccessNodeType(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(0)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }
        @Test
        void testAccessNodeProcessIdListIsEmpty() {
            accessNodeProcessIdList.remove(0);
            Assertions.assertDoesNotThrow(() -> dataSourceService.restoreAccessNodeType(updateDto, connectionDto, accessNodeProcessIdList));
            verify(connectionDto, times(0)).getAccessNodeType();
            verify(updateDto, times(0)).setAccessNodeType(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessId(anyString());
            verify(updateDto, times(0)).setAccessNodeProcessIdList(accessNodeProcessIdList);
            verify(updateDto, times(0)).setAccessNodeTypeEmpty(false);
            verify(updateDto, times(1)).isAccessNodeTypeEmpty();
        }

    }
}