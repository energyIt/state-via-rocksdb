package tech.energyit.state.rocksdb;

import java.time.LocalDateTime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import tech.energyit.state.model.HalfTrade;
import tech.energyit.state.model.Trade;

public class AbstractIntegrationTest {

    private static final String DB_PATH = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "db" + System.nanoTime();
    private static RocksDB db;
    private static Options options;

    @BeforeAll
    public static void initAll() {
        RocksDB.loadLibrary();
        try {
            options = new Options()
                    .setCreateIfMissing(true)
                    .setUseDirectIoForFlushAndCompaction(true);
            db = RocksDB.open(options, DB_PATH);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Could not create DB", e);
        }
    }

    @AfterAll
    public static void closeAll() {
        db.close();
        options.close();
    }

    protected RocksDB getDb() {
        return db;
    }

    protected Trade createTrade(int tradeId) {
        return createTrade(tradeId, "MBR1", "MBR2");
    }


    protected Trade createTrade(int tradeId, String buyMember, String sellMember) {
        return new Trade(tradeId, "ACTI", tradeId, 100, 10000, LocalDateTime.now(), 1,
                new HalfTrade("AREA1", "ACC1", tradeId, "test text", "USR002", buyMember),
                new HalfTrade("AREA2", "ACC1", tradeId + 1, "test text", "USR006", sellMember)
        );

    }
}
