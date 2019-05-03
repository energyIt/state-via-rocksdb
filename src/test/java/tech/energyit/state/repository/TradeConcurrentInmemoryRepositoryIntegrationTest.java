package tech.energyit.state.repository;

import tech.energyit.state.inmemory.ConcurrentMapPool;

/**
 *
 * @author gregmil
 */
public class TradeConcurrentInmemoryRepositoryIntegrationTest extends AbstractTradeRepositoryIntegrationTest {

    @Override
    protected TradeRepository newRepo() {
        final ConcurrentMapPool mapPool = new ConcurrentMapPool();
        return new TradeRepository(mapPool, mapPool);
    }

    @Override
    protected TradeRepository reinit(TradeRepository originalRepo) {
        // realod not supported for inmemory pool
        return originalRepo;
    }

}
