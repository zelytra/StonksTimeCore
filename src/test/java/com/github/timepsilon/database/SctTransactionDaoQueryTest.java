package com.github.timepsilon.database;

import com.github.timepsilon.database.dao.SctTransactionDao;
import com.github.timepsilon.database.entity.SctTransactionEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SctTransactionDaoQueryTest {

    @TempDir
    Path tempDir;

    @Test
    void sumsAmountForItemWithinTimeWindow() {
        SctTransactionDao dao = new SctTransactionDao();
        dao.connect(() -> SqliteHelper.open(tempDir.resolve("query.db")));
        dao.createTable();

        UUID player = UUID.randomUUID();
        Instant now = Instant.now();
        String diamond = "minecraft:diamond";
        String iron = "minecraft:iron_ingot";

        dao.upsertAll(List.of(
                new SctTransactionEntry(now.minus(Duration.ofHours(1)), player, "Alice", diamond, 5, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(2)), player, "Alice", diamond, 4, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(10)), player, "Alice", diamond, 3, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(1)), player, "Alice", iron, 7, 0)
        ));

        // last 3h: the -1h and -2h diamond rows (5 + 4), not the -10h one
        assertEquals(9, dao.sumAmountForItemSince(diamond, 3));
        // last 24h: all three diamond rows
        assertEquals(12, dao.sumAmountForItemSince(diamond, 24));
        // other item is not counted
        assertEquals(7, dao.sumAmountForItemSince(iron, 3));
        // unknown item / non-positive window
        assertEquals(0, dao.sumAmountForItemSince("minecraft:gold_ingot", 24));
        assertEquals(0, dao.sumAmountForItemSince(diamond, 0));

        dao.flushAndClose();
    }

    @Test
    void sumsAmountByItemWithinTimeWindow() {
        SctTransactionDao dao = new SctTransactionDao();
        dao.connect(() -> SqliteHelper.open(tempDir.resolve("byitem.db")));
        dao.createTable();

        UUID player = UUID.randomUUID();
        Instant now = Instant.now();
        String diamond = "minecraft:diamond";
        String iron = "minecraft:iron_ingot";

        dao.upsertAll(List.of(
                new SctTransactionEntry(now.minus(Duration.ofHours(1)), player, "Alice", diamond, 5, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(2)), player, "Alice", diamond, 4, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(10)), player, "Alice", diamond, 100, 0),
                new SctTransactionEntry(now.minus(Duration.ofHours(1)), player, "Alice", iron, 7, 0)
        ));

        Map<String, Integer> last3h = dao.sumAmountByItemSince(3);
        assertEquals(2, last3h.size());
        assertEquals(9, last3h.get(diamond));
        assertEquals(7, last3h.get(iron));
        // ordered by amount descending
        assertEquals(List.of(diamond, iron), List.copyOf(last3h.keySet()));

        // 24h window also picks up the -10h diamond row
        assertEquals(109, dao.sumAmountByItemSince(24).get(diamond));
        // non-positive window -> empty
        assertTrue(dao.sumAmountByItemSince(0).isEmpty());

        dao.flushAndClose();
    }
}
