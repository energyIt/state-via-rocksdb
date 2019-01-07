import model.HalfTrade;
import model.Trade;
import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadWriteIntegrationTest {

    private static final String DB_PATH = "C:\\tmp\\db\\";
    private static RocksDB db;

    @BeforeAll
    public static void initAll() {
        RocksDB.loadLibrary();
        try {
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, DB_PATH);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Could not create DB", e);
        }
    }

    @AfterAll
    public static void closeAll() {
        db.close();
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
    public void unknownIdReturnNull() {
        final Reader reader = new Reader(db, WireType.TEXT);
        assertThat(reader.get(999999999, Trade.class)).isNull();
    }

    private static Trade createTrade(int tradeId) {
        return new Trade(tradeId, "ACTI", tradeId, 100, 10000, LocalDateTime.now(), 1,
                new HalfTrade("AREA1", "ACC1", tradeId, "test text", "USR002", "MBR1"),
                new HalfTrade("AREA2", "ACC1", tradeId + 1, "test text", "USR006", "MBR2")
        );

    }
}
