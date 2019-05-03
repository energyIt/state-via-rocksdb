package tech.energyit.state.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.list.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.energyit.state.model.Trade;
import tech.energyit.state.rocksdb.AbstractIntegrationTest;
import tech.energyit.state.rocksdb.RocksDbReadWriteIntegrationTest;

import static java.util.Collections.singletonList;

public abstract class AbstractTradeRepositoryIntegrationTest extends AbstractIntegrationTest {

    private static Logger LOG = LoggerFactory.getLogger(RocksDbReadWriteIntegrationTest.class);

    private TradeRepository tradeRepository;

    @BeforeEach
    void setUp() {
        tradeRepository = newRepo();
    }

    protected abstract TradeRepository newRepo();

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
        LOG.info("{} Trades updated in in {} ms, MEM:{}m", count, System.currentTimeMillis() - start, memory());
        for (int i = 0; i < 50; i++) {
            tradeRepository.tradesUpdated(singletonList(createTrade(i, "M500", "M500")));
        }
        start = System.currentTimeMillis();
        tradeRepository = reinit(tradeRepository);
        LOG.info("intialized in {} ms, MEM:{}m", System.currentTimeMillis() - start, memory());

        start = System.currentTimeMillis();
        ImmutableList<Trade> readTrades = tradeRepository.getByMemberId("M1");
        LOG.info("getByMemberId returned in {} ms (size={}), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
        Assertions.assertThat(readTrades).isNotEmpty();
        Assertions.assertThat(readTrades.size()).isGreaterThan(1000);

        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M321");
        LOG.info("getByMemberId returned in {} ms (size={}), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
        Assertions.assertThat(readTrades).isNotEmpty();
        Assertions.assertThat(readTrades.size()).isGreaterThan(100);

        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M500");
        LOG.info("getByMemberId returned in {} ms (size={}), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
        Assertions.assertThat(readTrades).isNotEmpty();
        Assertions.assertThat(readTrades.size()).isEqualTo(100);

        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M1");
        LOG.info("getByMemberId returned in {} ms (size={}) (2nd call), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M321");
        LOG.info("getByMemberId returned in {} ms (size={}) (2nd call), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
        start = System.currentTimeMillis();
        readTrades = tradeRepository.getByMemberId("M500");
        LOG.info("getByMemberId returned in {} ms (size={}) (2nd call), MEM:{}m", System.currentTimeMillis() - start, readTrades.size(), memory());
    }

    private long memory() {
        return Runtime.getRuntime().totalMemory() / (1024 * 1024);
    }

    protected abstract TradeRepository reinit(TradeRepository originalRepo);

    @Test
    public void unknownMemberGetsEmptyList() {
        Assertions.assertThat(tradeRepository.getByMemberId("NEVER")).isEmpty();
    }
}
