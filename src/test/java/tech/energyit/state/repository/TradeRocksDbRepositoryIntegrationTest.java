package tech.energyit.state.repository;

import net.openhft.chronicle.wire.WireType;
import tech.energyit.state.rocksdb.RocksDbReader;
import tech.energyit.state.rocksdb.RocksDbWriter;

/**
 *
 * @author gregmil
 */
public class TradeRocksDbRepositoryIntegrationTest extends AbstractTradeRepositoryIntegrationTest {

    @Override
    protected TradeRepository newRepo() {
        final WireType wireType = WireType.BINARY_LIGHT;
        final RocksDbWriter writer = new RocksDbWriter(getDb(), wireType);
        final RocksDbReader reader = new RocksDbReader(getDb(), wireType);
        return new TradeRepository(writer, reader);
    }

    @Override
    protected TradeRepository reinit(TradeRepository originalRepo) {
        TradeRepository tradeRepository = newRepo();
        System.gc();
        tradeRepository.init();
        return tradeRepository;
    }
}
