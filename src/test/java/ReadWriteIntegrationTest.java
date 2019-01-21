import java.time.LocalDateTime;
import java.util.List;

import model.HalfTrade;
import model.Trade;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadWriteIntegrationTest {

    private static Logger LOG = LoggerFactory.getLogger(ReadWriteIntegrationTest.class);

    private static final String DB_PATH = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "db";
    private static RocksDB db;
    private static Options options;

    @BeforeAll
    public static void initAll() {
        RocksDB.loadLibrary();
        try {
            options = new Options().setCreateIfMissing(true);
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

    @Test
    public void writtenTradeDatasMustBeLoadedFully() {
        WireType wireType = WireType.BINARY_LIGHT;
        final Writer writer = new Writer(db, wireType);
        final Reader reader = new Reader(db, wireType);

        int tradeId = 100;

        Trade trade = createTrade(tradeId);
        writer.put(trade.getTradeId(), trade);
        Trade loadedTrade = reader.get(tradeId, Trade.class);
        assertThat(loadedTrade).isEqualToComparingFieldByField(trade);

    }

    @Test
    public void writeAndRead100KEvents() {
        WireType wireType = WireType.BINARY_LIGHT;
        final Writer writer = new Writer(db, wireType);
        final Reader reader = new Reader(db, wireType);
        final int count = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            final Trade trade = createTrade(i);
            writer.put(trade.getTradeId(), trade);
        }
        assertThat(writer.lastSequence()).isGreaterThanOrEqualTo(count);
        LOG.info("{} events persisted in {} ms", count, System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        writer.flush();
        LOG.info("flushed in {} ms", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Trade loadedTrade = reader.get(i, Trade.class);
            assertThat(loadedTrade.getSell()).isNotNull();
            assertThat(loadedTrade.getTradeId()).isEqualTo(i);
        }
        LOG.info("{} events loaded in {} ms", count, System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        final int idFrom = 100;
        final int idTo = 999;
        final int batchSize = idTo - idFrom + 1;
        List<Trade> loadedTrades = reader.getAllBetween(idFrom, idTo, Trade.class);
        assertThat(loadedTrades).hasSize(batchSize);
        //        assertThat(loadedTrades.get(0).getTradeId()).isEqualTo(idFrom);
        //        assertThat(loadedTrades.get(idTo-idFrom).getTradeId()).isEqualTo(idTo);
        LOG.info("Batch of {} loaded in {} ms", batchSize, System.currentTimeMillis() - start);
    }

    @Test
    public void unknownIdReturnNull() {
        final Reader reader = new Reader(db, WireType.TEXT);
        assertThat(reader.get(999999999, Trade.class)).isNull();
    }

    @Test
    public void getAllBetweenMustReturnOnlySpecifiedRange() {
        WireType wireType = WireType.BINARY_LIGHT;
        final Writer writer = new Writer(db, wireType);
        final Reader reader = new Reader(db, wireType);

        for (int i = 0; i < 20; i++) {
            final Trade trade = createTrade(i);
            writer.put(trade.getTradeId(), trade);
        }

        List<Trade> loadedTrades = reader.getAllBetween(11, 15, Trade.class);
        assertThat(loadedTrades)
                .hasSize(5)
                .extracting(Trade::getTradeId).containsOnly(11L, 12L, 13L, 14L, 15L);
    }

    private static Trade createTrade(int tradeId) {
        return new Trade(tradeId, "ACTI", tradeId, 100, 10000, LocalDateTime.now(), 1,
                new HalfTrade("AREA1", "ACC1", tradeId, "test text", "USR002", "MBR1"),
                new HalfTrade("AREA2", "ACC1", tradeId + 1, "test text", "USR006", "MBR2")
        );

    }
}
