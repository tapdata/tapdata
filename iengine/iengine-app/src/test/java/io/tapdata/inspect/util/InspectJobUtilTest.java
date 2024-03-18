package io.tapdata.inspect.util;

import com.tapdata.entity.inspect.InspectDataSource;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.inspect.InspectTaskContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InspectJobUtilTest {

    @Nested
    class WrapFilterTest {
        List<QueryOperator> srcConditions;
        TapAdvanceFilter tapAdvanceFilter;
        QueryOperator operator;

        @BeforeEach
        void init() {
            operator = mock(QueryOperator.class);

            srcConditions = new ArrayList<>();
            tapAdvanceFilter = mock(TapAdvanceFilter.class);

            doNothing().when(tapAdvanceFilter).setOperators(srcConditions);
            doNothing().when(tapAdvanceFilter).setMatch(any(DataMap.class));
            doNothing().when(tapAdvanceFilter).setOperators(null);
        }

        void assertVerify(List<QueryOperator> s, int op, int getOperator, int getKey, int getValue) {
            when(operator.getOperator()).thenReturn(op);
            when(operator.getKey()).thenReturn("key");
            when(operator.getValue()).thenReturn("value");
            try(MockedStatic<TapAdvanceFilter> taf = mockStatic(TapAdvanceFilter.class);
                MockedStatic<InspectJobUtil> iju = mockStatic(InspectJobUtil.class)) {
                taf.when(TapAdvanceFilter::create).thenReturn(tapAdvanceFilter);
                iju.when(() -> InspectJobUtil.wrapFilter(s)).thenCallRealMethod();
                TapAdvanceFilter filter = InspectJobUtil.wrapFilter(s);
                Assertions.assertNotNull(filter);
                taf.verify(TapAdvanceFilter::create, times(1));
            }
            verify(tapAdvanceFilter, times(1)).setOperators(s);
            verify(tapAdvanceFilter, times(1)).setMatch(any(DataMap.class));
            verify(operator, times(getOperator)).getOperator();
            verify(operator, times(getKey)).getKey();
            verify(operator, times(getValue)).getValue();
        }

        @Test
        void testSrcConditionsIsNull() {
            assertVerify(null, 0, 0, 0, 0);
        }

        @Test
        void testSrcConditionsIsEmpty() {
            assertVerify(srcConditions, 5, 0, 0, 0);
        }

        @Test
        void testNormal() {
            srcConditions.add(operator);
            assertVerify(srcConditions, 5, 1, 1, 1);
        }
        @Test
        void testOpNotFive() {
            srcConditions.add(operator);
            assertVerify(srcConditions, 4, 1, 0, 0);
        }
    }

    @Nested
    class GetTapTableTest {
        InspectDataSource inspectDataSource;
        InspectTaskContext inspectTaskContext;
        TapTable tapTable;
        ClientMongoOperator mongoOperator;
        @BeforeEach
        void init() {
            inspectDataSource = mock(InspectDataSource.class);
            inspectTaskContext = mock(InspectTaskContext.class);
            tapTable = mock(TapTable.class);
            mongoOperator = mock(ClientMongoOperator.class);

            when(inspectDataSource.getConnectionId()).thenReturn("ConnectionId");
            when(inspectDataSource.getTable()).thenReturn("tableName");
            when(inspectTaskContext.getClientMongoOperator()).thenReturn(mongoOperator);
        }
        @Test
        void testInspectTaskContextIsNull() {
            when(mongoOperator.findOne(any(Map.class), anyString(), any(Class.class))).thenReturn(tapTable);
            try (MockedStatic<InspectJobUtil> iju = mockStatic(InspectJobUtil.class)) {
                iju.when(() -> InspectJobUtil.getTapTable(inspectDataSource, null)).thenCallRealMethod();
                TapTable tapTable = InspectJobUtil.getTapTable(inspectDataSource, null);
                Assertions.assertNotNull(tapTable);
                iju.verify(() -> InspectJobUtil.getTapTable(inspectDataSource, null), times(1));
            }
            verify(inspectDataSource, times(1)).getConnectionId();
            verify(inspectDataSource, times(1)).getTable();
            verify(inspectTaskContext, times(0)).getClientMongoOperator();
            verify(mongoOperator, times(0)).findOne(any(Map.class), anyString(), any(Class.class));
        }
        @Test
        void testFindOneTableIsNull() {
            when(mongoOperator.findOne(any(Map.class), anyString(), any(Class.class))).thenReturn(null);
            try (MockedStatic<InspectJobUtil> iju = mockStatic(InspectJobUtil.class)) {
                iju.when(() -> InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext)).thenCallRealMethod();
                TapTable tapTable = InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext);
                Assertions.assertNotNull(tapTable);
                iju.verify(() -> InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext), times(1));
            }
            verify(inspectDataSource, times(1)).getConnectionId();
            verify(inspectDataSource, times(1)).getTable();
            verify(inspectTaskContext, times(1)).getClientMongoOperator();
            verify(mongoOperator, times(1)).findOne(any(Map.class), anyString(), any(Class.class));
        }
        @Test
        void testTableAllCaseNotNull() {
            when(mongoOperator.findOne(any(Map.class), anyString(), any(Class.class))).thenReturn(tapTable);
            try (MockedStatic<InspectJobUtil> iju = mockStatic(InspectJobUtil.class)) {
                iju.when(() -> InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext)).thenCallRealMethod();
                TapTable tapTable = InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext);
                Assertions.assertNotNull(tapTable);
                iju.verify(() -> InspectJobUtil.getTapTable(inspectDataSource, inspectTaskContext), times(1));
            }
            verify(inspectDataSource, times(1)).getConnectionId();
            verify(inspectDataSource, times(1)).getTable();
            verify(inspectTaskContext, times(1)).getClientMongoOperator();
            verify(mongoOperator, times(1)).findOne(any(Map.class), anyString(), any(Class.class));
        }
    }


    @Nested
    class CheckRowCountInspectTaskDataSourceTest {
        InspectDataSource dataSource;
        @BeforeEach
        void init() {
            dataSource = mock(InspectDataSource.class);
        }

        void assertVerify(InspectDataSource ds, int times, int size) {
            List<String> list = InspectJobUtil.checkRowCountInspectTaskDataSource("prefix", ds);
            Assertions.assertNotNull(list);
            Assertions.assertEquals(size, list.size());
            verify(dataSource, times(times)).getConnectionId();
            verify(dataSource, times(times)).getTable();
        }

        @Test
        void testDataSourceIsNull() {
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(null, 0, 1);
        }

        @Test
        void testConnectionIdIsNull() {
            when(dataSource.getConnectionId()).thenReturn(null);
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(dataSource, 1, 1);
        }

        @Test
        void testTableIsNull() {
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn(null);
            assertVerify(dataSource, 1, 1);
        }

        @Test
        void testAllCaseIsSucceed() {
            when(dataSource.getConnectionId()).thenReturn("id");
            when(dataSource.getTable()).thenReturn("table");
            assertVerify(dataSource, 1, 0);
        }

        @Test
        void testAllCaseIsFailed() {
            when(dataSource.getConnectionId()).thenReturn(null);
            when(dataSource.getTable()).thenReturn(null);
            assertVerify(dataSource, 1, 2);
        }
    }
}
