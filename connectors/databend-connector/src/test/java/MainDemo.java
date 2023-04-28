import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class MainDemo {
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:databend://localhost:8000", "root", "root");
        Statement statement = connection.createStatement();
        statement.execute("SELECT number from numbers(200000) order by number");
        ResultSet r = statement.getResultSet();
        r.next();
        for (int i = 1; i < 1000; i++) {
            r.next();
            System.out.println(r.getInt(1));
        }
        connection.close();
        TestSql();
    }

    public static void TestSql() {
        Set<String> fields = new HashSet<String> ();
        fields.add("x");
        fields.add("y");
        StringBuilder sql = new StringBuilder("INSERT");
        sql.append(" INTO ").append(sqlQuota(".", "db", "table")).append("(");

        for (String field : fields) sql.append(sqlQuota(field)).append(",");
        sql.setLength(sql.length() - 1);
        sql.append(")");

        sql.append(" VALUES(");
        for (String ignore : fields) sql.append("?,");
        sql.setLength(sql.length() - 1);
        sql.append(")");
        System.out.println(sql.toString());
    }
    private static final char QUOTA = '`';

    public static String sqlQuota(String delimiter, String... names) {
        return QUOTA + String.join(QUOTA + delimiter + QUOTA, names) + QUOTA;
    }
    public static String sqlQuota(String name) {
        return QUOTA + name + QUOTA;
    }
}
