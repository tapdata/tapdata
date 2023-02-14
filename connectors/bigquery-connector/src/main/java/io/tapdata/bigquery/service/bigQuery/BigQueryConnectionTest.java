package io.tapdata.bigquery.service.bigQuery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import io.tapdata.bigquery.enums.BigQueryTestItem;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;

import static io.tapdata.base.ConnectorBase.testItem;

public class BigQueryConnectionTest extends BigQueryStart {
    private static final String TAG = BigQueryConnectionTest.class.getSimpleName();

    public BigQueryConnectionTest(TapConnectionContext connectorContext) {
        super(connectorContext);
    }

    public static BigQueryConnectionTest create(TapConnectionContext connectorContext) {
        return new BigQueryConnectionTest(connectorContext);
    }

    public TestItem testServiceAccount() {
        String serviceAccount = this.config.serviceAccount();
        try {
            String tableSet = this.config.tableSet();
            SqlMarker sqlMarker = SqlMarker.create(serviceAccount);
            test(tableSet, sqlMarker);
            return testItem(BigQueryTestItem.TEST_SERVICE_ACCOUNT.getTxt(), TestItem.RESULT_SUCCESSFULLY, "Your credentials is available that. ");
        } catch (Exception e) {
            return testItem(BigQueryTestItem.TEST_SERVICE_ACCOUNT.getTxt(), TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public TestItem testTableSet() {
        try {
            String tableSet = this.config.tableSet();
            String serviceAccount = this.config.serviceAccount();
            return datasetExists(tableSet, SqlMarker.create(serviceAccount));
        } catch (Exception e) {
            return testItem(BigQueryTestItem.TEST_TABLE_SET.getTxt(), TestItem.RESULT_FAILED, String.format("Unable to get dataset, failure information: %s.", e.getMessage()));
        }
    }

    public TestItem datasetExists(String datasetName, SqlMarker sqlMarker) {
        try {
            return test(datasetName, sqlMarker);
        } catch (BigQueryException e) {
            return testItem(BigQueryTestItem.TEST_TABLE_SET.getTxt(), TestItem.RESULT_FAILED, "Something went wrong. " + e.getMessage());
        }
    }

    public TestItem test(String datasetName, SqlMarker sqlMarker) throws BigQueryException {
        try {
            Dataset dataset = sqlMarker.query().getDataset(DatasetId.of(datasetName));
            if (dataset != null) {
                return testItem(BigQueryTestItem.TEST_TABLE_SET.getTxt(), TestItem.RESULT_SUCCESSFULLY, "Dataset already exists.");
            } else {
                return testItem(BigQueryTestItem.TEST_TABLE_SET.getTxt(), TestItem.RESULT_FAILED, "Dataset not found.");
            }
        } catch (Exception e) {
            return testItem(BigQueryTestItem.TEST_TABLE_SET.getTxt(), TestItem.RESULT_FAILED, String.format("Unable to get dataset, failure information: %s.", e.getMessage()));
        }
    }
}
