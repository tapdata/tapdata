package io.tapdata.quickapi.server.enums;

public enum QuickApiTestItem {
    TEST_PARAM("Test connection params"),
    TEST_JSON_FORMAT("Check Api JSON"),
    TEST_TAP_TABLE("Check TAP_TABLE label format"),
    TEST_TOKEN("Check Token environment variable"),
    DEBUG_API("Attempt to call %s");
    String testName;
    QuickApiTestItem(String testName){
        this.testName = testName;
    }
    public String testName(){
        return this.testName;
    }

    public static void main(String[] args) {
        String s ="(regex()(.*?)())";
        System.out.println("regex(1)".matches(s));
        System.out.println("regex()".matches(s));
        System.out.println("regex(dgfdg)".matches(s));
        System.out.println("regex(rsgd%][@@)".matches(s));
        System.out.println("tt(1)".matches(s));

        String s2= "(\\[)(.*?)(])";
        System.out.println("[1,2]".matches(s2));
        System.out.println("[1]".matches(s2));
        System.out.println("[2]".matches(s2));
        System.out.println("[,2]".matches(s2));
        System.out.println("[".matches(s2));
    }
}
