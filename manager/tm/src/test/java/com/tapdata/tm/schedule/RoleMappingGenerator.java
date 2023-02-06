package com.tapdata.tm.schedule;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

public class RoleMappingGenerator {

  public static void main(String[] args) {

    String str = "[\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"5d31aeedb953565ded04baed\"\n" +
            "    },\n" +
            "    \"principalId\": \"Views metadata\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"5d31aeedb953565ded04baee\"\n" +
            "    },\n" +
            "    \"principalId\": \"Data Explorer\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"5d31b4d9a78f47de880f6241\"\n" +
            "    },\n" +
            "    \"principalId\": \"Dashboard\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc5009d4958d013d97ce2e\"\n" +
            "    },\n" +
            "    \"principalId\": \"62bc5008d4958d013d97c7a6\",\n" +
            "    \"principalType\": \"USER\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    }\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021fec\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"datasource\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021fed\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Data_SYNC\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021fee\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Data_verify\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021fef\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_catalog\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff0\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_catalog_all_data\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff1\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_quality\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff2\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_rules\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff3\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"time_to_live\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff4\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_lineage\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff5\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff6\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_data_explorer\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff7\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_doc_test\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff8\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_stats\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ff9\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_clients\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ffa\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_server\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ffb\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_collect\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ffc\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"schedule_jobs\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ffd\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Cluster_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021ffe\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"agents\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125021fff\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"user_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022000\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"role_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022001\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"system_settings\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022002\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"datasource\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022003\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Data_SYNC\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022004\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Data_verify\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022005\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_catalog\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022006\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_catalog_all_data\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022007\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_quality\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022008\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_rules\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022009\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"time_to_live\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200a\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_lineage\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200b\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200c\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_data_explorer\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200d\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_doc_test\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200e\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_stats\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b12502200f\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_clients\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022010\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"API_server\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022011\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"data_collect\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022012\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"schedule_jobs\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022013\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"Cluster_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022014\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"agents\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022015\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"user_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022016\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"role_management\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500a6beb33b125022017\"\n" +
            "    },\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"principalId\": \"system_settings\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d20c\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_government\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d217\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_government_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d220\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d228\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d230\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_publish\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d234\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_publish_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d23a\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d243\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d24f\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_export\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d255\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_deleting\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d25b\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_time_zone_editing\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d264\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d26a\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_download\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d26f\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_tagging\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d274\"\n" +
            "    },\n" +
            "    \"principalId\": \"notice_settings\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5d31ae1ab953565ded04badd\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d595\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_search\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d59e\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_search_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d5a5\"\n" +
            "    },\n" +
            "    \"principalId\": \"shared_cache\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d5b9\"\n" +
            "    },\n" +
            "    \"principalId\": \"shared_cache_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d5ca\"\n" +
            "    },\n" +
            "    \"principalId\": \"custom_node\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d5dc\"\n" +
            "    },\n" +
            "    \"principalId\": \"custom_node_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d5f2\"\n" +
            "    },\n" +
            "    \"principalId\": \"function_manager\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d601\"\n" +
            "    },\n" +
            "    \"principalId\": \"function_manager_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d60f\"\n" +
            "    },\n" +
            "    \"principalId\": \"log_collector\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d614\"\n" +
            "    },\n" +
            "    \"principalId\": \"log_collector_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d619\"\n" +
            "    },\n" +
            "    \"principalId\": \"notice\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d621\"\n" +
            "    },\n" +
            "    \"principalId\": \"notice_settings\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d62b\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d641\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d666\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_category_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d676\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_category_application\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d682\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d69f\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d6c0\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d6ce\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d6e3\"\n" +
            "    },\n" +
            "    \"principalId\": \"datasource_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d6f7\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_transmission_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d70e\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_transmission\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d723\"\n" +
            "    },\n" +
            "    \"principalId\": \"Data_SYNC_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d742\"\n" +
            "    },\n" +
            "    \"principalId\": \"Data_SYNC_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d74d\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_category_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d758\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_category_application\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d762\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d774\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d781\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d78d\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d79c\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d7ac\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_operation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d7c4\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_operation_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d7d6\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_import\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d7e7\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_job_export\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d7f7\"\n" +
            "    },\n" +
            "    \"principalId\": \"SYNC_Function_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d80a\"\n" +
            "    },\n" +
            "    \"principalId\": \"Data_verify_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d82a\"\n" +
            "    },\n" +
            "    \"principalId\": \"Data_verify_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d833\"\n" +
            "    },\n" +
            "    \"principalId\": \"verify_job_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d843\"\n" +
            "    },\n" +
            "    \"principalId\": \"verify_job_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d852\"\n" +
            "    },\n" +
            "    \"principalId\": \"verify_job_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d868\"\n" +
            "    },\n" +
            "    \"principalId\": \"verify_job_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d87a\"\n" +
            "    },\n" +
            "    \"principalId\": \"verify_job_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d88f\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_government_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d8a6\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_government\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d8c6\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d90b\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_category_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d923\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_category_application\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d932\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d93e\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_catalog_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d94b\"\n" +
            "    },\n" +
            "    \"principalId\": \"meta_data_deleting\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d953\"\n" +
            "    },\n" +
            "    \"principalId\": \"meta_data_deleting_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d965\"\n" +
            "    },\n" +
            "    \"principalId\": \"new_model_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d980\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_quality_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d9a1\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_quality_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d9b1\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_quality_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d9bf\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_quality_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97d9cf\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_rules_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97da27\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_rules_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97da53\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_rule_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97da5d\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_rule_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97da68\"\n" +
            "    },\n" +
            "    \"principalId\": \"time_to_live_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97da8b\"\n" +
            "    },\n" +
            "    \"principalId\": \"time_to_live_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dab4\"\n" +
            "    },\n" +
            "    \"principalId\": \"time_to_live_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dac4\"\n" +
            "    },\n" +
            "    \"principalId\": \"time_to_live_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dacd\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_lineage_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97daed\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_publish_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dafe\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_publish\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db0b\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_management_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db25\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db40\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_category_application\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db49\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_category_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db54\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db63\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db6e\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db88\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97db97\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dba8\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_publish\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dbbc\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_publish_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dbc7\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_import\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dbd0\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_export\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dbdb\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dbfb\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_export\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500ad4958d013d97dc04\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_deleting\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc17\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_time_zone_editing\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc23\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc2e\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_download\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc3b\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_data_explorer_tagging\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc46\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_doc_test_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc60\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_doc_test_export\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc70\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_stats_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dc92\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_clients_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dcac\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_clients_amangement\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dcb5\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_server_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dcc2\"\n" +
            "    },\n" +
            "    \"principalId\": \"API_server_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dccb\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_collect_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dce1\"\n" +
            "    },\n" +
            "    \"principalId\": \"data_collect_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dcf2\"\n" +
            "    },\n" +
            "    \"principalId\": \"system_management_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd04\"\n" +
            "    },\n" +
            "    \"principalId\": \"system_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd16\"\n" +
            "    },\n" +
            "    \"principalId\": \"schedule_jobs_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd28\"\n" +
            "    },\n" +
            "    \"principalId\": \"schedule_jobs_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd32\"\n" +
            "    },\n" +
            "    \"principalId\": \"Cluster_management_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd51\"\n" +
            "    },\n" +
            "    \"principalId\": \"Cluster_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd60\"\n" +
            "    },\n" +
            "    \"principalId\": \"Cluster_operation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd75\"\n" +
            "    },\n" +
            "    \"principalId\": \"status_log\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd7c\"\n" +
            "    },\n" +
            "    \"principalId\": \"agents_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dd91\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_management_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dda8\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97ddbd\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97ddcf\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dddb\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dde5\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97ddf1\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de00\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_category_management\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de0c\"\n" +
            "    },\n" +
            "    \"principalId\": \"user_category_application\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de17\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_management_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de25\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_management_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de32\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_creation\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de39\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_edition\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de42\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_edition_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de4d\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_delete\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de5b\"\n" +
            "    },\n" +
            "    \"principalId\": \"role_delete_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de71\"\n" +
            "    },\n" +
            "    \"principalId\": \"system_settings_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97de94\"\n" +
            "    },\n" +
            "    \"principalId\": \"system_settings_modification\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97deb2\"\n" +
            "    },\n" +
            "    \"principalId\": \"chart\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97decc\"\n" +
            "    },\n" +
            "    \"principalId\": \"chart_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97dee1\"\n" +
            "    },\n" +
            "    \"principalId\": \"dictionary_menu\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97deee\"\n" +
            "    },\n" +
            "    \"principalId\": \"dictionary\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  },\n" +
            "  {\n" +
            "    \"_id\": {\n" +
            "      \"$oid\": \"62bc500bd4958d013d97def8\"\n" +
            "    },\n" +
            "    \"principalId\": \"dictionary_all_data\",\n" +
            "    \"principalType\": \"PERMISSION\",\n" +
            "    \"roleId\": {\n" +
            "      \"$oid\": \"5b9a0a383fcba02649524bf1\"\n" +
            "    },\n" +
            "    \"self_only\": false\n" +
            "  }\n" +
            "]";


    JSONArray jsonArray = JSONUtil.parseArray(str);

    JSONArray newArray = new JSONArray();
    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = jsonArray.getJSONObject(i);
      JSONObject roleIdObj = jsonObject.getJSONObject("roleId");
      String oid = roleIdObj.getStr("$oid");
      if (oid.equals("5b9a0a383fcba02649524bf1")) {

        jsonObject.remove("_id");
        roleIdObj.replace("$oid", "5d31ae1ab953565ded04badd");
        newArray.add(jsonObject);
      }
    }

    System.out.println(newArray.toString());
  }
}
