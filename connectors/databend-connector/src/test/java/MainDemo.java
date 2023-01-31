import java.sql.*;

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
    }
}
