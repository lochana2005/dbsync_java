package model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DatabaseSyncService {


    private static final String LOCAL_DB_URL = "jdbc:mysql://127.0.0.1:3306/you_database";
    private static final String LOCAL_DB_USER = "username";
    private static final String LOCAL_DB_PASS = "password_local";


    private static final String CLOUD_DB_URL = "jdbc:mysql://online_database_ip/database_name";
    private static final String CLOUD_DB_USER = "username";
    private static final String CLOUD_DB_PASS = "password_online_database";

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new SyncTask(), 0, 60000);
    }

    static class SyncTask extends TimerTask {
        @Override
        public void run() {
            try (Connection localConn = DriverManager.getConnection(LOCAL_DB_URL, LOCAL_DB_USER, LOCAL_DB_PASS);
                 Connection cloudConn = DriverManager.getConnection(CLOUD_DB_URL, CLOUD_DB_USER, CLOUD_DB_PASS)) {

                List<String> tables = getAllTables(localConn);

                for (String table : tables) {
                    syncTable(localConn, cloudConn, table);
                    syncTable(cloudConn, localConn, table);
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        private List<String> getAllTables(Connection conn) throws SQLException {
            List<String> tables = new ArrayList<>();
            String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            return tables;
        }


        private void syncTable(Connection source, Connection target, String table) throws SQLException {
            System.out.println("Syncing table: " + table);


            List<String> columns = getTableColumns(source, table);
            if (columns.isEmpty()) {
                System.out.println("Skipping `" + table + "`: No columns found!");
                return;
            }

            String columnList = columns.stream()
                    .map(col -> "`" + col + "`")  // Escape column names with backticks
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");


            String checksumQuery = "SELECT MD5(CONCAT_WS(',', " + columnList + ")) AS checksum, " + columnList +
                    " FROM `" + table + "`";

            try (Statement srcStmt = source.createStatement();
                 ResultSet rs = srcStmt.executeQuery(checksumQuery)) {

                while (rs.next()) {
                    String checksum = rs.getString("checksum");
                    List<String> values = new ArrayList<>();


                    for (String column : columns) {
                        String value = rs.getString(column);
                        values.add(value != null ? "'" + value.replace("'", "\\'") + "'" : "NULL");
                    }


                    String insertQuery = "INSERT IGNORE INTO `" + table + "` (" + columnList + ") VALUES (" + String.join(", ", values) + ")";

                    try (Statement insertStmt = target.createStatement()) {
                        insertStmt.executeUpdate(insertQuery);
                        System.out.println("Inserted missing record into `" + table + "`");
                    }
                }
            }
        }


        private List<String> getTableColumns(Connection conn, String table) throws SQLException {
            List<String> columns = new ArrayList<>();
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }

            if (columns.isEmpty()) {
                System.err.println("Warning: No columns found for `" + table + "`!");
            }

            return columns;
        }
    }
}
