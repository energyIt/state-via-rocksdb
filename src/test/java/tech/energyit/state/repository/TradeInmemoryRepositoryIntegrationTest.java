package tech.energyit.state.repository;

import tech.energyit.state.inmemory.MapPool;

/**
 *
 * @author gregmil
 */
public class TradeInmemoryRepositoryIntegrationTest extends AbstractTradeRepositoryIntegrationTest {

    @Override
    protected TradeRepository newRepo() {
        final MapPool mapPool = new MapPool();
        return new TradeRepository(mapPool, mapPool);
    }

    @Override
    protected TradeRepository reinit(TradeRepository originalRepo) {
        // realod not supported for inmemory pool
        return originalRepo;
    }

}
