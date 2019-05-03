package tech.energyit.state.repository;

import org.eclipse.collections.api.list.ImmutableList;
import tech.energyit.state.model.Trade;

/**
 * NOTE: Repositories are updated only from single thread.
 *       However, typically, many threads are reading.
 */
public class TradeRepository {

    private Writer writer;
    private Reader reader;

    private TradeMemberIndex tradeMemberIndex = new TradeMemberIndex();

    public TradeRepository(Writer writer, Reader reader) {
        this.writer = writer;
        this.reader = reader;
    }

    public void init() {
       reader.forEach(id -> true, t -> tradeMemberIndex.add(t),  Trade.class);
    }
    /**
     * atomically updates all trades in db and all indexes
     */
    public void tradesUpdated(Iterable<Trade> trades) {
        writer.putAll(Trade::getTradeId, trades);
        trades.forEach(t -> tradeMemberIndex.add(t));
    }

    public ImmutableList<Trade> getByMemberId(String memberId) {
        return tradeMemberIndex.tradeIdsFor(memberId).collect(id -> reader.get(id, Trade.class));
    }
}
