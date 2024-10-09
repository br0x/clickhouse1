package su.brox;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Random;

public class App {
    private static final Logger logger = LogManager.getLogger(App.class);
    private static final String JDBC_URL = "jdbc:clickhouse://localhost:8123/trades";
    private static final int NUM_TICKERS = 300_000_000;
    private static final int BATCH_SIZE = 100_000;
    private static final String[] INSTRUMENTS = {
            "btc_usd",
            "eth_usd",
            "matic_usd",
            "trc_usd",
            "ltc_usd",
            "ton_usd",
            "waves_usd",
            "bch_usd",
            "btc_eth",
            "btc_ltc",
    };

    private static final long startTime = 1725148800000L; // Start time: 2024-09-01 00:00:00 UTC

    public static void main(String[] args) {

        try (Connection connection = DriverManager.getConnection(JDBC_URL)) {
            logger.info("Connected to ClickHouse database");

            for (String i : INSTRUMENTS) {
                createTable(connection, i);
                insertData(connection, i);
            }

            for (String i : INSTRUMENTS) {
                queryData(connection, i);
            }
        } catch (SQLException e) {
            logger.error("Error connecting to ClickHouse database", e);
        }
    }

    private static void createTable(Connection connection, String instrument) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS ticker_" + instrument +
                " (timestamp DateTime64(3), " +
                "price Float64, " +
                "volume Float64) " +
                "ENGINE = MergeTree() " +
                "ORDER BY timestamp";

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            logger.info("Table 'ticker_{}' created successfully", instrument);
        }
    }

    private static void insertData(Connection connection, String instrument) throws SQLException {
        String insertDataSQL = "INSERT INTO ticker_" + instrument +
                " (timestamp, price, volume) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(insertDataSQL)) {
            double price = 0.5 + Math.random() * 99.5; // starting price between 0.5 and 100
            Random random = new Random();
            long lastTime = startTime;

            for (int i = 0; i < NUM_TICKERS; i++) {
                // Timestamp: increment by 80-90 milliseconds (average interval 85 ms)
                lastTime += (80 + random.nextInt(11));

                // Price: new_price = prev_price + prev_price * random(1) * 0.00001
                price += (price * random.nextDouble() * 0.00001);

                // Volume: random between 0.1 and 1000
                double volume = 0.1 + random.nextDouble() * 999.9;

                pstmt.setTimestamp(1, new Timestamp(lastTime));
                pstmt.setDouble(2, price);
                pstmt.setDouble(3, volume);
                pstmt.addBatch();

                if ((i + 1) % BATCH_SIZE == 0) {
                    pstmt.executeBatch();
                    pstmt.clearBatch();
                    logger.info("Inserted {} rows for instrument {}", i + 1, instrument);
                }
            }

            // Insert any remaining rows
            pstmt.executeBatch();
            logger.info("Finished inserting {} rows for instrument {}", NUM_TICKERS, instrument);
        }
    }

    private static void queryData(Connection connection, String instrument) throws SQLException {
        logger.info("=====================================");
        logger.info("Instrument: {}", instrument);
        String query = "SELECT * FROM ticker_" + instrument + " LIMIT 20";

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {
                long timestamp = resultSet.getTimestamp("timestamp").getTime();
                double price = resultSet.getDouble("price");
                double volume = resultSet.getDouble("volume");
                logger.info("Row: timestamp={}, price={}, volume={}", timestamp, price, volume);
            }
        }
    }
}
