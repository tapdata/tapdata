package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.util.http.Http;
import io.tapdata.bigquery.util.http.HttpEntity;
import io.tapdata.bigquery.util.http.HttpResult;
import io.tapdata.bigquery.util.http.HttpType;

import java.util.ArrayList;
import java.util.Map;

public class WriteRecord {

    public void connectBigQuery(String project,String dataset,String tableId){
        Http http = Http.create("https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll?key={key}"
                , HttpType.POST
                , HttpEntity.create()
                        .build("Authorization", "Bearer 34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
                        .build("X-goog-api-key","34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
                        .build("Content-Type","application/json; charset=utf-8")
        ).body(HttpEntity.create()
//                .build("kind","")
//                .build("skipInvalidRows","")
//                .build("ignoreUnknownValues","")
//                .build("templateSuffix","")
//                .build("traceId","")
                .build("rows",new ArrayList<Map<String,Object>>(){{
                    add(
                            HttpEntity.create()
                                    .build("insertId","1111111")
                                    .build("json", JSONUtil.toJsonStr(
                                            HttpEntity.create()
                                            .build("id","222")
                                            .build("name",1)
                                            .build("type",3.66f)
                                            .build("int",null)
                                            .build("num",null)
                                            .build("bigNum",null)
                                            .build("bool",null)
                                            .build("timestamp",null)
                                            .build("date",null)
                                            .build("dataetime",null)
                                            .build("map",null)
                                            .build("record",null)
                                            .build("json",null).entity()
                                            )
                                    ).entity()
                    );
                }})
        ).resetFull(HttpEntity.create()
                .build("projectId",project)
                .build("datasetId",dataset)
                .build("tableId",tableId)
                .build("key","-----BEGIN PRIVATE KEY-----MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYbcMZXMmiwO2y6eqPzGKr+CRAAOSNHyPZ0DFdbI5E4Oscg6UMCaaJLwfmD4VHMXD+1d1Y9X6LGDNTEvB0s9sjJvI2kPRLm7alqUcMuJkROBT3W39cryAaKd+1Q3yEu2P7FoVSZkgAbi3JNCU6jGjvzGVW3ei/aGRNVoFCCw8xnf3SoASMb+ZJvgsvGer+O/Lt1CTURjOzjDVZjcdFeuiT+rpLcBZGmP4TUXNuy3ZEXKWS81gKbMBzhIhtW8sELgl9r8I+hJrnlYDkD2py819Ef4bCuYJhU4rbrPpCKH5iauyty5IK1OPxZSs7vAfthLNMCPENzvo8A7Sr8mM2434rAgMBAAECggEABCx5O6ANS14SBCSgjhhwGTpdr7z2hSC1qBipyV+YE62+8lRueAJpo3b8teF16kmhyPCNM4rhUKi0exFZMTDdjrxZxIG6lrloSmf0sJX7ZvvMoytHtP98lwrPe9Shu7av2ae3tdZkIVLjAQ/i9xPyKaLEoZjI7zjKCk4UkvzfiSG6BVq/8IzadG7BV2qRLBRYRFrUo06WEy3f98eUDelKyUtFKwgtzCXwNUFWocBlvGLvJtHLQUYiGijwl+1KY+IlTOIllA7lwuG4R4ZvcPeSajppuDN8C3JuuRztGIvntHjIklMz1J9k/dhrbUAbmPOwfzKKr2mIbjVSgO4ZUU/66QKBgQDPV6YN2paQUKwZy0XJQDVAmH1qhbe9qEaGutqsIvnD1WVE0JFIdD2bklr0m2SRpEmiIFACE7SOtzIbA1byTndLkHkf8a+TKyajRI89V6U6GIAiSysUCvXESo5GyOkp/2rNro+AUUdYOtf7YgLHipW+fVDWFRjUTjmVN2ho901ziQKBgQC8MxwEWNwd8FaFutz2yAKClfLwf1BHbaRi2K7+GvcqOXnJ4/W5lsN5cbEbvKTO+NrM/M9oElb9jWGn2gWoQKhg6MxuM18Boh2HEOIdpGXX4TKvfSmJYX80aDoiCZ4EzRGE6SelnWrKzhpO7PpBBRhDW1/jV2fHqzQWbohfehrTEwKBgCohsE9eXHvkuKPhJ0QWtPt0QP/VPhneyL312BtkXAZMJXDPRMZJQH+NRMgxj0T88i1sjXVulaDuXtMYYaGJCjqjl8lC7h9khExm0Qhw99UPR3IwfgdrlrcVQ0Xk62QqT4SN9QDpAytNgbfGGbR8V6NGiZeG3+28G31TrfauUeGpAoGBALisWmC1pYFHVk+xlqQejcAATjzKYUdGApnwUH8OjN0FO0nuBDDSDQx9kLJMAVkLfwDJTuirnmr9sgcYfJamo9M8fWXhyOd8YgcofQljSYB1/duQMRMa9czCPdEqqMHDTN6kP4BXIPTTG6O5DLSCwFVQM56NJUwb5mfgnLc7xVi7AoGAXt7zdhrwvCRzG0WAcAz1\\niQhdK5VReebgG5fzRNse2Un2r/LUvMZ4MHE+yQgvHEoBtmO9K+WYgCQbSk0EYXxRRi2CeYKTLe6iIHibn0GmlEVF3f+ELUXHT/dFy24OZ2w0T04zeKJ6cJsj/oU5K9jh5a5e7bWHsVKWMklBfwkapz4=-----END PRIVATE KEY-----")
        );
        HttpResult result = http.http();
        System.out.println(result.getResult());
    }

    public static void main(String[] args) {
        WriteRecord record = new WriteRecord();
        record.connectBigQuery("vibrant-castle-366614","tableSet001","table1");
    }
}
