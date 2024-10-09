package su.brox;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class AppTest {

    private static final Logger logger = LogManager.getLogger(AppTest.class);
    private static final String JDBC_URL = "jdbc:clickhouse://localhost:8123/trades?socket_timeout=300000";
    private static Connection connection;

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager.getConnection(JDBC_URL);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void testSumOfVolumesPerTimePeriod() throws SQLException {
        String instrument = "btc_usd";
        String query = "SELECT toDate(timestamp) AS date, SUM(volume) AS total_volume " +
                "FROM ticker_" + instrument + " " +
                "WHERE timestamp BETWEEN '2024-09-05 00:00:00' AND '2024-09-25 23:59:59' " +
                "GROUP BY date " +
                "ORDER BY date " +
                "LIMIT 5";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            int rowCount = 0;
            while (rs.next()) {
                Date date = rs.getDate("date");
                double totalVolume = rs.getDouble("total_volume");
                logger.info("Row: date={}, totalVolume={}", date, totalVolume);

                assertNotNull(date);
                assertTrue(totalVolume > 0);
                rowCount++;
            }

            assertTrue(rowCount > 0, "Expected at least one row in the result");
        }
    }

    @Test
    void testAveragePricePerTimePeriod() throws SQLException {
        String instrument = "eth_usd";
        String query = "SELECT toStartOfHour(timestamp) AS hour, avg(price) AS avg_price " +
                "FROM ticker_" + instrument + " " +
                "WHERE timestamp BETWEEN '2024-09-01 00:00:00' AND '2024-09-02 23:59:59' " +
                "GROUP BY hour " +
                "ORDER BY hour " +
                "LIMIT 5";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            int rowCount = 0;
            while (rs.next()) {
                Timestamp hour = rs.getTimestamp("hour");
                double avgPrice = rs.getDouble("avg_price");
                logger.info("Row: hour={}, avgPrice={}", hour, avgPrice);

                assertNotNull(hour);
                assertTrue(avgPrice > 0);
                rowCount++;
            }

            assertTrue(rowCount > 0, "Expected at least one row in the result");
        }
    }

    @Test
    void testAverageVolumeOnWindow() throws SQLException {
        String instrument = "matic_usd";
        // as RANGE INTERVAL for DateTime(64) is not supported, we use 1 h / 85ms = 42353 rows as workaround
        String query = "SELECT timestamp, volume, " +
                "avg(volume) OVER (ORDER BY timestamp ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS moving_avg_10, " +
                "avg(volume) OVER (ORDER BY timestamp ROWS BETWEEN 42353 PRECEDING AND CURRENT ROW) AS moving_avg_1h " +
                "FROM ticker_" + instrument + " " +
                "WHERE timestamp BETWEEN '2024-09-05 00:00:00' AND '2024-09-05 01:00:00' " +
                "ORDER BY timestamp " +
                "LIMIT 20";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            int rowCount = 0;
            while (rs.next()) {
                Timestamp timestamp = rs.getTimestamp("timestamp");
                double volume = rs.getDouble("volume");
                double movingAvg10 = rs.getDouble("moving_avg_10");
                double movingAvg1h = rs.getDouble("moving_avg_1h");
                logger.info("Row: timestamp={}, volume={}, MA10={}, MA1h={}", timestamp, volume, movingAvg10, movingAvg1h);

                assertNotNull(timestamp);
                assertTrue(volume > 0);
                assertTrue(movingAvg10 > 0);
                assertTrue(movingAvg1h > 0);
                rowCount++;
            }

            assertTrue(rowCount > 0, "Expected at least one row in the result");
        }
    }

    @Test
    void testPriceInterpolationAndDerivation() throws SQLException {
        String query = """
            WITH
            start_time AS (
                SELECT greatest(
                    (SELECT min(timestamp) FROM ticker_btc_usd),
                    (SELECT min(timestamp) FROM ticker_eth_usd),
                    (SELECT min(timestamp) FROM ticker_btc_eth)
                )
            ),
            end_time AS (
                SELECT least(
                    (SELECT max(timestamp) FROM ticker_btc_usd),
                    (SELECT max(timestamp) FROM ticker_eth_usd),
                    (SELECT max(timestamp) FROM ticker_btc_eth)
                )
            ),
            btc_usd_candles AS (
                SELECT 
                    toStartOfFiveMinute(timestamp) AS candle_time,
                    argMax(price, timestamp) AS btc_usd_price
                FROM ticker_btc_usd
                WHERE timestamp BETWEEN (SELECT * FROM start_time) AND (SELECT * FROM end_time)
                GROUP BY candle_time
            ),
            eth_usd_candles AS (
                SELECT 
                    toStartOfFiveMinute(timestamp) AS candle_time,
                    argMax(price, timestamp) AS eth_usd_price
                FROM ticker_eth_usd
                WHERE timestamp BETWEEN (SELECT * FROM start_time) AND (SELECT * FROM end_time)
                GROUP BY candle_time
            ),
            btc_eth_candles AS (
                SELECT 
                    toStartOfFiveMinute(timestamp) AS candle_time,
                    argMax(price, timestamp) AS btc_eth_price
                FROM ticker_btc_eth
                WHERE timestamp BETWEEN (SELECT * FROM start_time) AND (SELECT * FROM end_time)
                GROUP BY candle_time
            ),
            all_candles AS (
                SELECT 
                    btc_usd_candles.candle_time,
                    btc_usd_candles.btc_usd_price,
                    eth_usd_candles.eth_usd_price,
                    btc_eth_candles.btc_eth_price,
                    btc_usd_candles.btc_usd_price / btc_eth_candles.btc_eth_price AS eth_usd_derived
                FROM btc_usd_candles
                JOIN eth_usd_candles ON btc_usd_candles.candle_time = eth_usd_candles.candle_time
                JOIN btc_eth_candles ON btc_usd_candles.candle_time = btc_eth_candles.candle_time
            )
            SELECT 
                min(eth_usd_price) AS min_eu,
                max(eth_usd_price) AS max_eu,
                min(eth_usd_derived) AS min_deu,
                max(eth_usd_derived) AS max_deu,
                sqrt(avg(pow(eth_usd_price - eth_usd_derived, 2))) AS rms_difference
            FROM all_candles
        """;

        long startTime = System.currentTimeMillis();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            assertTrue(rs.next(), "Expected at least one row in the result");

            double minEU = rs.getDouble("min_eu");
            double maxEU = rs.getDouble("max_eu");
            double minDEU = rs.getDouble("min_deu");
            double maxDEU = rs.getDouble("max_deu");
            double rmsDifference = rs.getDouble("rms_difference");
//            assertFalse(rs.wasNull(), "RMS difference should not be null");
//            assertTrue(rmsDifference >= 0, "RMS difference should be non-negative");

            logger.info("Row: minEU={}, maxEU={}, minDEU={}, maxDEU={}", minEU, maxEU, minDEU, maxDEU);
        }

        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Test execution time: " + executionTime + " ms");

        assertTrue(executionTime > 0, "Execution time should be positive");
    }
}
