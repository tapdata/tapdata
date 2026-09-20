package com.tapdata.tm.trace.service.data;

import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DataTrace reverse-filter cases aligned with:
 * <ul>
 *   <li>MDM merge task 6a8e4e95b2c69173e185978c (FDM_* -&gt; MDM_HBL_TT)</li>
 *   <li>FDM clone task 6a90f52fe409db2ba24a7fc7 (pg_fdm.hbl_* -&gt; FDM_pg_hbl_*)</li>
 * </ul>
 */
class TraceUpstreamConditionRewriterTest {

    private static final Map<String, String> ACCOUNT_FIELDS = Map.of(
            "account_id", "account_id",
            "customer_id", "customer_id",
            "account_number", "account_number"
    );
    private static final Map<String, String> TXN_FIELDS = Map.of(
            "transaction_id", "transaction_id",
            "account_id", "account_id",
            "amount", "amount"
    );
    private static final Map<String, String> CUSTOMER_FIELDS = Map.of(
            "customer_id", "customer_id",
            "customer_type", "customer_type",
            "first_name", "first_name"
    );
    private static final Map<String, String> WIDE_TABLE_FIELDS = Map.of(
            "customer_id", "customer_id",
            "ACCOUNT.account_id", "account_id",
            "ACCOUNT.TRANSACTION.transaction_id", "transaction_id"
    );

    @Nested
    @DisplayName("MDM 6a8e4e95: merge sub-table reverse when wide table misses")
    class MdmMergeSubTable {

        @Test
        void accountPath_shouldMapNestedAccountId() {
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                            List.of(filter("ACCOUNT.account_id", "acc-000001")),
                            "ACCOUNT",
                            List.of(mapping("customer_id", "customer_id")),
                            List.of(mapping("account_id", "account_id")),
                            ACCOUNT_FIELDS
                    )
            );
        }

        @Test
        void transactionJoinKey_shouldMapWideTableAccountId() {
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                            List.of(filter("ACCOUNT.account_id", "acc-000001")),
                            "ACCOUNT.TRANSACTION",
                            List.of(mapping("account_id", "ACCOUNT.account_id")),
                            List.of(mapping("transaction_id", "transaction_id")),
                            TXN_FIELDS
                    )
            );
        }

        @Test
        void transactionPath_shouldMapNestedTransactionId() {
            assertEquals(
                    List.of(Map.of("transaction_id", "txn-000211")),
                    TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                            List.of(filter("ACCOUNT.TRANSACTION.transaction_id", "txn-000211")),
                            "ACCOUNT.TRANSACTION",
                            List.of(mapping("account_id", "ACCOUNT.account_id")),
                            Collections.emptyList(),
                            TXN_FIELDS
                    )
            );
        }

        @Test
        void accountPath_shouldIgnoreTransactionOnlyField() {
            assertTrue(TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                    List.of(filter("ACCOUNT.TRANSACTION.transaction_id", "txn-000211")),
                    "ACCOUNT",
                    List.of(mapping("customer_id", "customer_id")),
                    Collections.emptyList(),
                    ACCOUNT_FIELDS
            ).isEmpty());
        }

        @Test
        void tablePk_shouldMapWhenJoinKeyDoesNotMatch() {
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                            List.of(filter("ACCOUNT.account_id", "acc-000001")),
                            null,
                            Collections.emptyList(),
                            List.of(mapping("account_id", "ACCOUNT.account_id")),
                            ACCOUNT_FIELDS
                    )
            );
        }
    }

    @Nested
    @DisplayName("MDM 6a8e4e95: main table must not inherit sub-table filters")
    class MdmMainTable {

        @Test
        void customer_shouldSkipNestedAccountId() {
            assertTrue(TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                    List.of(filter("ACCOUNT.account_id", "acc-000001")),
                    WIDE_TABLE_FIELDS,
                    CUSTOMER_FIELDS
            ).isEmpty());
        }

        @Test
        void customer_shouldKeepCustomerId() {
            assertEquals(
                    List.of(Map.of("customer_id", "cust-000001")),
                    TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                            List.of(filter("customer_id", "cust-000001")),
                            WIDE_TABLE_FIELDS,
                            CUSTOMER_FIELDS
                    )
            );
        }

        @Test
        void nestedOnlyFilter_shouldBeDetected() {
            assertTrue(TraceUpstreamConditionRewriter.onlyNestedFilterKeys(
                    List.of(filter("ACCOUNT.account_id", "acc-000001"))));
            assertFalse(TraceUpstreamConditionRewriter.onlyNestedFilterKeys(
                    List.of(filter("customer_id", "cust-000001"))));
            assertFalse(TraceUpstreamConditionRewriter.onlyNestedFilterKeys(Collections.emptyList()));
        }

        @Test
        void accountRecords_shouldRebuildCustomerFilterByJoinKey() {
            Map<String, Object> account = new LinkedHashMap<>();
            account.put("account_id", "acc-000001");
            account.put("customer_id", "cust-001775");
            assertEquals(
                    List.of(Map.of("customer_id", "cust-001775")),
                    TraceUpstreamConditionRewriter.rewriteMainTableFiltersFromSubTableRecords(
                            List.of(account),
                            List.of(mapping("customer_id", "customer_id")),
                            CUSTOMER_FIELDS
                    )
            );
        }

        @Test
        void transactionRecords_shouldNotRebuildCustomerFilter() {
            Map<String, Object> txn = new LinkedHashMap<>();
            txn.put("account_id", "acc-000001");
            txn.put("transaction_id", "txn-000211");
            assertTrue(TraceUpstreamConditionRewriter.rewriteMainTableFiltersFromSubTableRecords(
                    List.of(txn),
                    List.of(mapping("account_id", "ACCOUNT.account_id")),
                    CUSTOMER_FIELDS
            ).isEmpty());
        }

        @Test
        void duplicateAccountRecords_shouldDedupCustomerFilter() {
            Map<String, Object> a1 = filter("customer_id", "cust-001775");
            a1.put("account_id", "acc-000001");
            Map<String, Object> a2 = filter("customer_id", "cust-001775");
            a2.put("account_id", "acc-002095");
            List<Map<String, Object>> result = TraceUpstreamConditionRewriter.rewriteMainTableFiltersFromSubTableRecords(
                    List.of(a1, a2),
                    List.of(mapping("customer_id", "customer_id")),
                    CUSTOMER_FIELDS
            );
            assertEquals(1, result.size());
            assertEquals("cust-001775", result.get(0).get("customer_id"));
        }
    }

    @Nested
    @DisplayName("MDM 6a8e4e95: merge siblings fill each other from records")
    class MdmMergeSiblings {

        private static final List<FieldNameMapping> ACCOUNT_JOIN_KEYS = List.of(mapping("customer_id", "customer_id"));
        private static final List<FieldNameMapping> ACCOUNT_PK = List.of(mapping("account_id", "account_id"));
        private static final List<FieldNameMapping> TXN_JOIN_KEYS = List.of(mapping("account_id", "ACCOUNT.account_id"));
        private static final List<FieldNameMapping> TXN_PK = List.of(mapping("transaction_id", "transaction_id"));
        private static final List<FieldNameMapping> CUSTOMER_PK = List.of(mapping("customer_id", "customer_id"));

        @Test
        void accountRecords_shouldRebuildTransactionFilterByAccountId() {
            Map<String, Object> account = new LinkedHashMap<>();
            account.put("account_id", "acc-000001");
            account.put("customer_id", "cust-001775");
            account.put("status", "ACTIVE");
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(account),
                            ACCOUNT_JOIN_KEYS,
                            ACCOUNT_PK,
                            TXN_JOIN_KEYS,
                            TXN_PK,
                            TXN_FIELDS
                    )
            );
        }

        @Test
        void transactionRecords_shouldRebuildAccountFilterByAccountId() {
            Map<String, Object> txn = new LinkedHashMap<>();
            txn.put("account_id", "acc-000001");
            txn.put("transaction_id", "txn-000211");
            txn.put("amount", 12.5);
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(txn),
                            TXN_JOIN_KEYS,
                            TXN_PK,
                            ACCOUNT_JOIN_KEYS,
                            ACCOUNT_PK,
                            ACCOUNT_FIELDS
                    )
            );
        }

        @Test
        void customerRecords_shouldRebuildAccountFilterByCustomerId() {
            assertEquals(
                    List.of(Map.of("customer_id", "cust-001775")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(filter("customer_id", "cust-001775")),
                            Collections.emptyList(),
                            CUSTOMER_PK,
                            ACCOUNT_JOIN_KEYS,
                            ACCOUNT_PK,
                            ACCOUNT_FIELDS
                    )
            );
        }

        @Test
        void customerRecords_shouldNotRebuildTransactionFilter() {
            assertTrue(TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                    List.of(filter("customer_id", "cust-001775")),
                    Collections.emptyList(),
                    CUSTOMER_PK,
                    TXN_JOIN_KEYS,
                    TXN_PK,
                    TXN_FIELDS
            ).isEmpty());
        }

        @Test
        void transactionRecords_shouldNotRebuildCustomerFilter() {
            Map<String, Object> txn = new LinkedHashMap<>();
            txn.put("account_id", "acc-000001");
            txn.put("transaction_id", "txn-000211");
            assertTrue(TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                    List.of(txn),
                    TXN_JOIN_KEYS,
                    TXN_PK,
                    Collections.emptyList(),
                    CUSTOMER_PK,
                    CUSTOMER_FIELDS
            ).isEmpty());
        }

        @Test
        void accountRecords_shouldStillRebuildCustomerFilter() {
            Map<String, Object> account = new LinkedHashMap<>();
            account.put("account_id", "acc-000001");
            account.put("customer_id", "cust-001775");
            assertEquals(
                    List.of(Map.of("customer_id", "cust-001775")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(account),
                            ACCOUNT_JOIN_KEYS,
                            ACCOUNT_PK,
                            Collections.emptyList(),
                            CUSTOMER_PK,
                            CUSTOMER_FIELDS
                    )
            );
        }

        @Test
        void duplicateAccountIds_shouldDedupTransactionFilter() {
            Map<String, Object> a1 = filter("account_id", "acc-000001");
            a1.put("customer_id", "cust-001775");
            Map<String, Object> a2 = filter("account_id", "acc-000001");
            a2.put("customer_id", "cust-001775");
            List<Map<String, Object>> result = TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                    List.of(a1, a2),
                    ACCOUNT_JOIN_KEYS,
                    ACCOUNT_PK,
                    TXN_JOIN_KEYS,
                    TXN_PK,
                    TXN_FIELDS
            );
            assertEquals(1, result.size());
            assertEquals("acc-000001", result.get(0).get("account_id"));
        }
    }

    @Nested
    @DisplayName("OMS DT: renamed join key customer_no -> cust_no")
    class OmsRenamedJoinKey {

        private static final Map<String, String> OMS_CUSTOMER_FIELDS = Map.of(
                "cust_no", "cust_no",
                "member_level", "member_level",
                "full_name", "full_name"
        );
        private static final Map<String, String> OMS_ORDER_FIELDS = Map.of(
                "order_no", "order_no",
                "customer_no", "customer_no",
                "pay_amount", "pay_amount"
        );

        @Test
        void orderRecords_shouldRebuildCustomerFilterByRenamedJoinKey() {
            Map<String, Object> order = new LinkedHashMap<>();
            order.put("order_no", "oms-o-0140");
            order.put("customer_no", "oms-c-0060");
            assertEquals(
                    List.of(Map.of("cust_no", "oms-c-0060")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(order),
                            List.of(mapping("customer_no", "cust_no")),
                            List.of(mapping("order_no", "order_no")),
                            Collections.emptyList(),
                            List.of(mapping("cust_no", "cust_no")),
                            OMS_CUSTOMER_FIELDS
                    )
            );
        }

        @Test
        void customerRecords_shouldRebuildOrderFilterByRenamedJoinKey() {
            assertEquals(
                    List.of(Map.of("customer_no", "oms-c-0060")),
                    TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                            List.of(filter("cust_no", "oms-c-0060")),
                            Collections.emptyList(),
                            List.of(mapping("cust_no", "cust_no")),
                            List.of(mapping("customer_no", "cust_no")),
                            List.of(mapping("order_no", "order_no")),
                            OMS_ORDER_FIELDS
                    )
            );
        }
    }

    @Nested
    @DisplayName("FDM 6a90f52f: 1:1 clone pg_fdm.hbl_* -> FDM_pg_hbl_*")
    class FdmClone {

        @Test
        void account_shouldKeepAccountIdOnSameField() {
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                            List.of(filter("account_id", "acc-000001")),
                            ACCOUNT_FIELDS,
                            ACCOUNT_FIELDS
                    )
            );
        }

        @Test
        void customer_shouldKeepCustomerIdOnSameField() {
            assertEquals(
                    List.of(Map.of("customer_id", "cust-001775")),
                    TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                            List.of(filter("customer_id", "cust-001775")),
                            CUSTOMER_FIELDS,
                            CUSTOMER_FIELDS
                    )
            );
        }

        @Test
        void transaction_shouldKeepAccountIdJoinFromParent() {
            assertEquals(
                    List.of(Map.of("account_id", "acc-000001")),
                    TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                            List.of(filter("account_id", "acc-000001")),
                            TXN_FIELDS,
                            TXN_FIELDS
                    )
            );
        }
    }

    @Nested
    @DisplayName("Guard / empty inputs")
    class Guards {

        @Test
        void emptyFilters_shouldReturnEmpty() {
            assertTrue(TraceUpstreamConditionRewriter.rewriteMergeSubTableFilters(
                    Collections.emptyList(), "ACCOUNT", Collections.emptyList(), Collections.emptyList(), ACCOUNT_FIELDS
            ).isEmpty());
            assertTrue(TraceUpstreamConditionRewriter.rewriteNormalUpstreamFilters(
                    Collections.emptyList(), WIDE_TABLE_FIELDS, CUSTOMER_FIELDS
            ).isEmpty());
            assertTrue(TraceUpstreamConditionRewriter.rewriteMainTableFiltersFromSubTableRecords(
                    Collections.emptyList(), List.of(mapping("customer_id", "customer_id")), CUSTOMER_FIELDS
            ).isEmpty());
            assertTrue(TraceUpstreamConditionRewriter.rewriteSiblingFiltersFromRecords(
                    Collections.emptyList(),
                    List.of(mapping("customer_id", "customer_id")),
                    List.of(mapping("account_id", "account_id")),
                    List.of(mapping("account_id", "ACCOUNT.account_id")),
                    List.of(mapping("transaction_id", "transaction_id")),
                    TXN_FIELDS
            ).isEmpty());
        }

        @Test
        void stripNestedPath_shouldCoverEdges() {
            assertEquals("account_id", TraceUpstreamConditionRewriter.stripNestedPath("ACCOUNT.account_id", "ACCOUNT"));
            assertEquals("TRANSACTION.transaction_id",
                    TraceUpstreamConditionRewriter.stripNestedPath("ACCOUNT.TRANSACTION.transaction_id", "ACCOUNT"));
            assertNull(TraceUpstreamConditionRewriter.stripNestedPath("ACCOUNT", "ACCOUNT"));
            assertNull(TraceUpstreamConditionRewriter.stripNestedPath("customer_id", "ACCOUNT"));
            assertNull(TraceUpstreamConditionRewriter.stripNestedPath("ACCOUNT.account_id", null));
        }
    }

    private static Map<String, Object> filter(String key, Object value) {
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put(key, value);
        return filter;
    }

    private static FieldNameMapping mapping(String originName, String targetName) {
        FieldNameMapping mapping = new FieldNameMapping();
        mapping.setOriginName(originName);
        mapping.setTargetName(targetName);
        return mapping;
    }
}
