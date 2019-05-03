package tech.energyit.state.rocksdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.openhft.chronicle.wire.WireType;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.energyit.state.model.Trade;

import static org.assertj.core.api.Assertions.assertThat;

public class RocksDbReadWriteIntegrationTest extends AbstractIntegrationTest {

    private static Logger LOG = LoggerFactory.getLogger(RocksDbReadWriteIntegrationTest.class);

    @Test
    public void writtenTradeDatasMustBeLoadedFully() {
        WireType wireType = WireType.BINARY_LIGHT;
        try (final RocksDbWriter writer = new RocksDbWriter(getDb(), wireType);
                final RocksDbReader reader = new RocksDbReader(getDb(), wireType)) {

            int tradeId = 100;

            Trade trade = createTrade(tradeId);
            writer.put(trade.getTradeId(), trade);
            Trade loadedTrade = reader.get(tradeId, Trade.class);
            assertThat(loadedTrade).isEqualToComparingFieldByField(trade);
        }
    }

    @Test
    public void writtenTradesInBatchMustBeLoadedFully() {
        WireType wireType = WireType.BINARY_LIGHT;
        try (final RocksDbWriter writer = new RocksDbWriter(getDb(), wireType);
                final RocksDbReader reader = new RocksDbReader(getDb(), wireType)) {

            List<Trade> trades = Arrays.asList(createTrade(1), createTrade(2), createTrade(3));
            writer.putAll(Trade::getTradeId, trades);
            for (Trade writtenTrade : trades) {
                Trade loadedTrade = reader.get(writtenTrade.getTradeId(), Trade.class);
                assertThat(loadedTrade).isEqualToComparingFieldByField(writtenTrade);
            }
        }
    }

    @Test
    public void writeAndRead100KEvents() throws RocksDBException {
        WireType wireType = WireType.BINARY_LIGHT;
        try (final RocksDbWriter writer = new RocksDbWriter(getDb(), wireType);
                final RocksDbReader reader = new RocksDbReader(getDb(), wireType)) {
            final int count = 100000;
            long start = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                final Trade trade = createTrade(i);
                writer.put(trade.getTradeId(), trade);
            }
            assertThat(writer.lastSequence()).isGreaterThanOrEqualTo(count);
            LOG.info("{} events persisted in {} ms", count, System.currentTimeMillis() - start);

            start = System.currentTimeMillis();
            List<Trade> batch = new ArrayList<>(100);
            for (int i = 0; i < count; i++) {
                final Trade trade = createTrade(i);
                if (i % 100 == 0) {
                    writer.putAll(Trade::getTradeId, batch);
                    batch.clear();
                } else {
                    batch.add(trade);
                }
                writer.put(trade.getTradeId(), trade);
            }
            assertThat(writer.lastSequence()).isGreaterThanOrEqualTo(count);
            LOG.info("{} events persisted via 100#batches in {} ms", count, System.currentTimeMillis() - start);

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

            start = System.currentTimeMillis();
            getDb().compactRange();
            LOG.info("db compacted in {} ms", System.currentTimeMillis() - start);
        }
    }

    @Test
    public void unknownIdReturnNull() {
        final RocksDbReader reader = new RocksDbReader(getDb(), WireType.TEXT);
        assertThat(reader.get(999999999, Trade.class)).isNull();
    }

    @Test
    public void getAllBetweenMustReturnOnlySpecifiedRange() {
        WireType wireType = WireType.BINARY_LIGHT;
        try (final RocksDbWriter writer = new RocksDbWriter(getDb(), wireType);
                final RocksDbReader reader = new RocksDbReader(getDb(), wireType)) {

            for (int i = 0; i < 20; i++) {
                final Trade trade = createTrade(i);
                writer.put(trade.getTradeId(), trade);
            }

            List<Trade> loadedTrades = reader.getAllBetween(11, 15, Trade.class);
            assertThat(loadedTrades)
                    .hasSize(5)
                    .extracting(Trade::getTradeId).containsOnly(11L, 12L, 13L, 14L, 15L);
        }
    }
}
