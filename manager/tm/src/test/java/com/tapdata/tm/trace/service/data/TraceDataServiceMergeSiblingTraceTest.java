package com.tapdata.tm.trace.service.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.analyzer.entity.LineageTableNode;
import com.tapdata.tm.trace.dto.TaskLineageDto;
import com.tapdata.tm.trace.dto.TraceQueryCondition;
import com.tapdata.tm.trace.dto.TraceStreamEvent;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import com.tapdata.tm.trace.dto.boodline.TableProperties;
import com.tapdata.tm.trace.enums.TraceEventType;
import com.tapdata.tm.trace.param.TraceFilter;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import io.tapdata.pdk.apis.entity.QueryOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TraceDataServiceMergeSiblingTraceTest {

    private static final String MDM_CONN = "mdm-conn";
    private static final String FDM_CONN = "fdm-conn";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private BloodlineFinder bloodlineFinder;
    @Mock
    private TraceDataQueryRpcAdapter queryAdapter;

    private TraceDataService service;
    private LineageTableNode target;
    private LineageTableNode customer;
    private LineageTableNode account;
    private LineageTableNode transaction;

    @BeforeEach
    void setUp() {
        service = new TraceDataService();
        ReflectionTestUtils.setField(service, "bloodlineFinder", bloodlineFinder);
        ReflectionTestUtils.setField(service, "objectMapper", MAPPER);
        ReflectionTestUtils.setField(service, "traceDataQueryAdapter", queryAdapter);

        target = table(MDM_CONN, "MDM_HBL_TT");
        customer = mergeTable(FDM_CONN, "FDM_pg_hbl_customer", "mainTable", null,
                List.of(), List.of(mapping("customer_id", "customer_id")));
        account = mergeTable(FDM_CONN, "FDM_pg_hbl_account", "subTable", "ACCOUNT",
                List.of(mapping("customer_id", "customer_id")),
                List.of(mapping("account_id", "account_id")));
        transaction = mergeTable(FDM_CONN, "FDM_pg_hbl_transaction", "subTable", "ACCOUNT.TRANSACTION",
                List.of(mapping("account_id", "ACCOUNT.account_id")),
                List.of(mapping("transaction_id", "transaction_id")));

        TaskLineageDto lineage = new TaskLineageDto(new Dag(
                List.of(
                        new Edge(customer.getId(), target.getId()),
                        new Edge(account.getId(), target.getId()),
                        new Edge(transaction.getId(), target.getId())
                ),
                List.of(target, customer, account, transaction)
        ));
        lineage.setUpdateConditionFieldList(new HashMap<>());
        lineage.setFieldNameMapping(Map.of(
                target.getId(), Map.of(
                        "customer_id", "customer_id",
                        "ACCOUNT.account_id", "account_id",
                        "ACCOUNT.TRANSACTION.transaction_id", "transaction_id"
                ),
                customer.getId(), Map.of("customer_id", "customer_id", "customer_type", "customer_type"),
                account.getId(), Map.of("account_id", "account_id", "customer_id", "customer_id"),
                transaction.getId(), Map.of("transaction_id", "transaction_id", "account_id", "account_id")
        ));
        when(bloodlineFinder.findTaskLineage(any())).thenReturn(lineage);
    }

    @Test
    @DisplayName("M5: customer_id miss should fill account then transaction")
    void customerIdMiss_shouldFillAccountAndTransaction() throws Exception {
        when(queryAdapter.query(any())).thenAnswer(invocation -> recordsFor(invocation.getArgument(0)));

        List<TraceStreamEvent> events = trace("customer_id", "cust-001775");

        assertTrue(tracedTables(events).containsAll(List.of(
                "MDM_HBL_TT", "FDM_pg_hbl_customer", "FDM_pg_hbl_account", "FDM_pg_hbl_transaction"
        )));
        assertFalse(hasFilterNotBuilt(events), () -> "unexpected errors: " + errorCodes(events));
        assertTrue(matchedCount(events, "FDM_pg_hbl_transaction") > 0);
    }

    @Test
    @DisplayName("M7: nested transaction_id miss should fill transaction then account and customer")
    void nestedTransactionIdMiss_shouldFillAccountAndCustomer() throws Exception {
        when(queryAdapter.query(any())).thenAnswer(invocation -> recordsFor(invocation.getArgument(0)));

        List<TraceStreamEvent> events = trace("ACCOUNT.TRANSACTION.transaction_id", "txn-000211");

        assertTrue(tracedTables(events).containsAll(List.of(
                "MDM_HBL_TT", "FDM_pg_hbl_transaction", "FDM_pg_hbl_account", "FDM_pg_hbl_customer"
        )));
        assertFalse(hasFilterNotBuilt(events), () -> "unexpected errors: " + errorCodes(events));
        assertTrue(matchedCount(events, "FDM_pg_hbl_account") > 0);
        assertTrue(matchedCount(events, "FDM_pg_hbl_customer") > 0);
    }

    private List<Map<String, Object>> recordsFor(TraceQueryCondition condition) {
        String table = condition.getTable();
        if ("MDM_HBL_TT".equals(table)) {
            return List.of();
        }
        if ("FDM_pg_hbl_customer".equals(table) && hasValue(condition, "customer_id", "cust-001775")) {
            return List.of(Map.of("customer_id", "cust-001775", "customer_type", "BUSINESS"));
        }
        if ("FDM_pg_hbl_account".equals(table)
                && (hasValue(condition, "customer_id", "cust-001775") || hasValue(condition, "account_id", "acc-000001"))) {
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("account_id", "acc-000001");
            record.put("customer_id", "cust-001775");
            return List.of(record);
        }
        if ("FDM_pg_hbl_transaction".equals(table)
                && (hasValue(condition, "account_id", "acc-000001") || hasValue(condition, "transaction_id", "txn-000211"))) {
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("transaction_id", "txn-000211");
            record.put("account_id", "acc-000001");
            return List.of(record);
        }
        return List.of();
    }

    private List<TraceStreamEvent> trace(String key, String value) throws Exception {
        WideTableTraceRequest request = new WideTableTraceRequest();
        request.setConnectionId(MDM_CONN);
        request.setTable("MDM_HBL_TT");
        TraceFilter filters = new TraceFilter();
        QueryOperator custom = new QueryOperator();
        custom.setKey(key);
        custom.setValue(value);
        filters.setCustom(List.of(custom));
        request.setFilters(filters);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        service.traceData(request, "trace_test", output);
        List<TraceStreamEvent> events = new ArrayList<>();
        for (String line : output.toString(StandardCharsets.UTF_8).split("\n")) {
            if (!line.isBlank()) {
                events.add(MAPPER.readValue(line, TraceStreamEvent.class));
            }
        }
        return events;
    }

    private static boolean hasValue(TraceQueryCondition condition, String key, Object value) {
        if (condition == null || condition.getFilters() == null) {
            return false;
        }
        return condition.getFilters().stream().anyMatch(filter -> Objects.equals(filter.get(key), value));
    }

    private static Set<String> tracedTables(List<TraceStreamEvent> events) {
        return events.stream()
                .filter(event -> event.getType() == TraceEventType.TRACE_VALUE)
                .map(TraceStreamEvent::getTable)
                .collect(Collectors.toSet());
    }

    private static boolean hasFilterNotBuilt(List<TraceStreamEvent> events) {
        return events.stream()
                .filter(event -> event.getType() == TraceEventType.NODE_ERROR)
                .map(TraceStreamEvent::getError)
                .filter(Objects::nonNull)
                .anyMatch(error -> "Trace.Filter.NotBuilt".equals(error.getCode()));
    }

    private static List<String> errorCodes(List<TraceStreamEvent> events) {
        return events.stream()
                .filter(event -> event.getType() == TraceEventType.NODE_ERROR)
                .map(event -> event.getTable() + ":" + (event.getError() == null ? null : event.getError().getCode()))
                .collect(Collectors.toList());
    }

    private static long matchedCount(List<TraceStreamEvent> events, String table) {
        return events.stream()
                .filter(event -> event.getType() == TraceEventType.TRACE_VALUE)
                .filter(event -> table.equals(event.getTable()))
                .map(TraceStreamEvent::getTraceValue)
                .filter(Objects::nonNull)
                .mapToLong(value -> value.getMatchedCount())
                .sum();
    }

    private static LineageTableNode table(String connectionId, String tableName) {
        return new LineageTableNode(tableName, connectionId, connectionId, null, null);
    }

    private static LineageTableNode mergeTable(String connectionId, String tableName, String tableType, String path,
                                               List<FieldNameMapping> joinKeys, List<FieldNameMapping> tablePk) {
        LineageTableNode node = table(connectionId, tableName);
        TableProperties properties = new TableProperties();
        properties.setNodeType("MERGE");
        properties.setTableType(tableType);
        properties.setPath(path);
        properties.setJoinKeys(joinKeys);
        properties.setTablePk(tablePk);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("merge", properties);
        node.setAttrs(attrs);
        return node;
    }

    private static FieldNameMapping mapping(String originName, String targetName) {
        FieldNameMapping mapping = new FieldNameMapping();
        mapping.setOriginName(originName);
        mapping.setTargetName(targetName);
        return mapping;
    }
}
