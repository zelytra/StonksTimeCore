package com.github.timepsilon.database.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.timepsilon.database.SqliteHelper;
import com.github.timepsilon.database.entity.SctTransactionEntry;
import com.github.timepsilon.database.pending.PendingWriteQueue;
import com.github.timepsilon.database.pending.PendingWritesStore;

import com.github.timepsilon.utils.TimeCore;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SctTransactionDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(SctTransactionDao.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS sct_transaction (
                time TEXT NOT NULL,
                player TEXT NOT NULL,
                username TEXT NOT NULL,
                item TEXT NOT NULL,
                amount INTEGER NOT NULL,
                money INTEGER NOT NULL,
                PRIMARY KEY (time, player, item)
            )
            """;

    private static final String UPSERT = """
            INSERT INTO sct_transaction
            (time, player, username, item, amount, money)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (time, player, item)
            DO UPDATE SET
            amount = sct_transaction.amount + excluded.amount,
            money = sct_transaction.money + excluded.money,
            username = excluded.username
            """;

    private static final String SUM_AMOUNT_SINCE = """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM sct_transaction
            WHERE item = ? AND time >= ?
            """;

    private static final String SUM_AMOUNT_BY_ITEM_SINCE = """
            SELECT item, COALESCE(SUM(amount), 0) AS total
            FROM sct_transaction
            WHERE time >= ?
            GROUP BY item
            ORDER BY total DESC
            """;

    private final PendingWriteQueue<SctTransactionEntry> pending = PendingWritesStore.get().sctTransactions();

    private @Nullable Connection connection;
    private @Nullable PreparedStatement upsertStatement;
    private @Nullable BankDao.ConnectionSupplier connectionSupplier;

    public void connect() {
        connect(connectionSupplier);
    }

    public void connect(BankDao.ConnectionSupplier supplier) {
        connectionSupplier = supplier;
        try {
            if (connection != null && !connection.isClosed()) return;
            connection = supplier.get();
            LOGGER.debug("Connected to SQLite (table={}).", SctTransactionEntry.TABLE_NAME);
            createTable();
            tryFlushPending();
        } catch (SQLException e) {
            LOGGER.error("Error loading SCT transaction database!", e);
            disconnect();
        }
    }

    public void createTable() {
        if (connection == null) return;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE);
            upsertStatement = connection.prepareStatement(UPSERT);
        } catch (Exception e) {
            LOGGER.error("Error creating SCT transaction table.", e);
            disconnect();
        }
    }

    /**
     * Returns the total {@code amount} of {@code itemId} sold over the last {@code hours} hours.
     * <p>
     * Timestamps are stored as hourly buckets, so the window is hour-granular. Returns 0 when there
     * is no connection, {@code hours <= 0}, or on error. Call from the server thread (shared JDBC
     * connection, not thread-safe).
     */
    public int sumAmountForItemSince(String itemId, int hours) {
        if (hours <= 0) return 0;
        ensureConnected();
        if (connection == null) return 0;
        Instant since = TimeCore.getCurrentInstant().minus(Duration.ofHours(hours));
        try (PreparedStatement statement = connection.prepareStatement(SUM_AMOUNT_SINCE)) {
            statement.setString(1, itemId);
            statement.setString(2, SqliteHelper.toIso(since));
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getInt("total") : 0;
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to sum SCT amount for item {}", itemId, e);
            if (SqliteHelper.isConnectionError(e)) {
                disconnect();
            }
            return 0;
        }
    }

    /**
     * Returns the total {@code amount} sold per item over the last {@code hours} hours, keyed by the
     * stored item id (as produced by {@code Item.toString()}), ordered by amount descending.
     * <p>
     * Timestamps are stored as hourly buckets, so the window is hour-granular. Returns an empty map
     * when there is no connection, {@code hours <= 0}, or on error. Call from the server thread.
     */
    public Map<String, Integer> sumAmountByItemSince(int hours) {
        if (hours <= 0) return Map.of();
        ensureConnected();
        if (connection == null) return Map.of();
        Instant since = TimeCore.getCurrentInstant().minus(Duration.ofHours(hours));
        Map<String, Integer> totals = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(SUM_AMOUNT_BY_ITEM_SINCE)) {
            statement.setString(1, SqliteHelper.toIso(since));
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    totals.put(resultSet.getString("item"), resultSet.getInt("total"));
                }
            }
            return totals;
        } catch (SQLException e) {
            LOGGER.error("Failed to sum SCT amounts by item", e);
            if (SqliteHelper.isConnectionError(e)) {
                disconnect();
            }
            return Map.of();
        }
    }

    public void upsertAll(Collection<SctTransactionEntry> entries) {
        if (entries.isEmpty()) return;
        ensureConnected();
        if (!isReady()) {
            pending.enqueueAll(entries);
            LOGGER.debug(
                    "Pending write enqueued (no connection): table={}, batch={}, queueSize={}",
                    SctTransactionEntry.TABLE_NAME, entries.size(), pending.size()
            );
            PendingWritesStore.get().persistToDisk();
            return;
        }
        if (!executeBatch(entries)) {
            pending.enqueueAll(entries);
            LOGGER.debug(
                    "Pending write enqueued (write failed): table={}, batch={}, queueSize={}",
                    SctTransactionEntry.TABLE_NAME, entries.size(), pending.size()
            );
        }
    }

    public boolean tryFlushPending() {
        if (pending.isEmpty()) return true;
        ensureConnected();
        if (!isReady()) return false;

        List<SctTransactionEntry> toFlush = pending.drainAll();
        LOGGER.info("Flushing {} pending SCT transaction write(s).", toFlush.size());
        if (executeBatch(toFlush)) {
            PendingWritesStore.get().persistToDisk();
            return true;
        }
        pending.enqueueAll(toFlush);
        PendingWritesStore.get().persistToDisk();
        return false;
    }

    public void flushAndClose() {
        tryFlushPending();
        try {
            if (upsertStatement != null) {
                int[] results = upsertStatement.executeBatch();
                LOGGER.debug(
                        "SQL batch flushed on close: table={}, count={}",
                        SctTransactionEntry.TABLE_NAME, results.length
                );
                upsertStatement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to flush transactions to database!", e);
        }
        disconnect();
    }

    private void ensureConnected() {
        if (isReady()) {
            try {
                if (connection.isClosed()) {
                    disconnect();
                }
            } catch (SQLException e) {
                disconnect();
            }
        }
        if (!isReady()) {
            connect(connectionSupplier);
        }
    }

    private boolean isReady() {
        return connection != null && upsertStatement != null;
    }

    private boolean executeBatch(Collection<SctTransactionEntry> entries) {
        if (upsertStatement == null) return false;
        try {
            for (SctTransactionEntry entry : entries) {
                LOGGER.debug(
                        "SQL upsert batch: table={}, player={}, username={}, time={}, item={}, amount={}, money={}",
                        SctTransactionEntry.TABLE_NAME, entry.player(), entry.username(), entry.time(),
                        entry.itemId(), entry.amount(), entry.moneyAsFloat()
                );
                entry.bindTo(upsertStatement);
                upsertStatement.addBatch();
            }
            int[] results = upsertStatement.executeBatch();
            LOGGER.debug(
                    "SQL batch executed: table={}, count={}",
                    SctTransactionEntry.TABLE_NAME, results.length
            );
            upsertStatement.clearBatch();
            return true;
        } catch (SQLException e) {
            LOGGER.error("Failed to send transactions to database!", e);
            if (SqliteHelper.isConnectionError(e)) {
                LOGGER.warn(
                        "SQLite connection lost (table={}), will retry pending writes.",
                        SctTransactionEntry.TABLE_NAME
                );
            }
            disconnect();
            return false;
        }
    }

    private void disconnect() {
        try {
            if (upsertStatement != null) {
                upsertStatement.close();
                upsertStatement = null;
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                connection = null;
                LOGGER.debug("Disconnected from SQLite (table={}).", SctTransactionEntry.TABLE_NAME);
            }
        } catch (SQLException e) {
            LOGGER.error("Error closing SCT transaction database!", e);
        }
    }
}
