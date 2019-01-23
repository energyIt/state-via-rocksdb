package repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import model.Trade;
import net.openhft.chronicle.wire.WireType;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.list.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocksdb.AbstractIntegrationTest;
import rocksdb.ReadWriteIntegrationTest;
import rocksdb.Reader;
import rocksdb.Writer;

public class TradeRepositoryIntegrationTest extends AbstractIntegrationTest {

    private static Logger LOG = LoggerFactory.getLogger(ReadWriteIntegrationTest.class);

    private TradeRepository tradeRepository;

    @BeforeEach
    void setUp() {
        tradeRepository = newRepo();
    }

    private TradeRepository newRepo() {
        final WireType wireType = WireType.BINARY_LIGHT;
        final Writer writer = new Writer(getDb(), wireType);
        final Reader reader = new Reader(getDb(), wireType);
        return new TradeRepository(writer, reader);
    }

    @Test
    public void updatedTradesCanBeAccessedByMember() {
        tradeRepository.tradesUpdated(Arrays.asList(
                createTrade(1, "M1", "M2"),
                createTrade(2, "M1", "M3"),
                createTrade(3, "M1", "M4")
        ));

        Assertions.assertThat(tradeRepository.getByMemberId("M1")).extracting(Trade::getTradeId).containsOnly(1L, 2L, 3L);
        Assertions.assertThat(tradeRepository.getByMemberId("M2")).extracting(Trade::getTradeId).containsOnly(1L);
        Assertions.assertThat(tradeRepository.getByMemberId("M3")).extracting(Trade::getTradeId).containsOnly(2L);
        Assertions.assertThat(tradeRepository.getByMemberId("M4")).extracting(Trade::getTradeId).containsOnly(3L);
    }

    @Test
    public void getByMemberIdMustBeFast() {
        long start = System.currentTimeMillis();
        List<Trade> batch = new ArrayList<>(1_000);
        int count = 1_000_000;
        for (int i = 0; i < count; i++) {
            final Trade trade = createTrade(i, "M" + i % 431, "M" + i % 91);
            if (i % 1_000 == 0) {
                tradeRepository.tradesUpdated(batch);
                batch.clear();
            } else {
                batch.add(trade);
            }
        }
        LOG.info("{} Trades updated in in {} ms", count, System.currentTimeMillis() - start);

        tradeRepository = newRepo();
        System.gc();
        start = System.currentTimeMillis();
        tradeRepository.init();
        LOG.info("intialized in {} ms", System.currentTimeMillis() - start);

        start = System.currentTimeMillis();
        ImmutableList<Trade> readTrades = tradeRepository.getByMemberId("M1");
        LOG.info("getByMemberId returned in {} ms (size={})", System.currentTimeMillis() - start, readTrades.size());
        Assertions.assertThat(readTrades).isNotEmpty();
        Assertions.assertThat(readTrades.size()).isGreaterThan(9000);

        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M321");
        LOG.info("getByMemberId returned in {} ms (size={})", System.currentTimeMillis() - start, readTrades.size());
        Assertions.assertThat(readTrades).isNotEmpty();
        Assertions.assertThat(readTrades.size()).isGreaterThan(1000);
    }

    @Test
    public void unknownMemberGetsEmptyList() {
        Assertions.assertThat(tradeRepository.getByMemberId("NEVER")).isEmpty();
    }
}
