package io.tapdata.bigquery.enums;

public enum BigQueryTestItem {
    TEST_SERVICE_ACCOUNT("Check your Credentials "),
    TEST_TABLE_SET("Check your table set ")
    ;
    String txt;
    BigQueryTestItem(String txt){
        this.txt = txt;
    }

    public String getTxt() {
        return txt;
    }

    public void setTxt(String txt) {
        this.txt = txt;
    }
}
