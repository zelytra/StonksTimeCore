package com.github.timepsilon.database;

import com.github.timepsilon.config.SqlStatsGate;
import com.github.timepsilon.database.dao.SctTransactionDao;
import com.github.timepsilon.database.entity.SctTransactionEntry;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.world.item.Item;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SCTTransactionDatabase {

    private static final SCTTransactionDatabase DATABASE = new SCTTransactionDatabase();

    private final SctTransactionDao dao = new SctTransactionDao();

    private SCTTransactionDatabase() {}

    public void load(MinecraftServer server) {
        if (!SqlStatsGate.isEnabled()) return;
        Path databaseFile = SqliteHelper.databaseFile(server);
        dao.connect(() -> SqliteHelper.open(databaseFile));
        dao.tryFlushPending();
    }

    public void unload() {
        if (!SqlStatsGate.isEnabled()) return;
        dao.flushAndClose();
    }

    public void flushPending() {
        if (!SqlStatsGate.isEnabled()) return;
        dao.tryFlushPending();
    }

    public void sendTransactions(ServerPlayer player, Map<Item, Integer> amountMap, Map<Item, Float> moneyMap) {
        if (!SqlStatsGate.isEnabled()) return;
        List<SctTransactionEntry> entries = new ArrayList<>();
        for (Map.Entry<Item, Integer> entry : amountMap.entrySet()) {
            Item item = entry.getKey();
            entries.add(SctTransactionEntry.from(
                    player,
                    item,
                    entry.getValue(),
                    moneyMap.getOrDefault(item, 0.0f)
            ));
        }
        dao.upsertAll(entries);
    }

    /**
     * Total quantity of {@code item} sold via the chronoscope over the last {@code hours} hours.
     * Returns 0 when SQL stats are disabled. Call from the server thread.
     */
    public int getAmountSoldForItem(Item item, int hours) {
        if (!SqlStatsGate.isEnabled()) return 0;
        return dao.sumAmountForItemSince(item.toString(), hours);
    }

    /**
     * Quantity sold per item over the last {@code hours} hours, keyed by item id (as produced by
     * {@code Item.toString()}) and ordered by amount descending. Empty when SQL stats are disabled.
     * Call from the server thread.
     */
    public Map<String, Integer> getAmountsSoldByItem(int hours) {
        if (!SqlStatsGate.isEnabled()) return Map.of();
        return dao.sumAmountByItemSince(hours);
    }

    public static SCTTransactionDatabase getDatabase() {
        return DATABASE;
    }
}
