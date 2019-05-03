package tech.energyit.state.repository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.jetbrains.annotations.NotNull;
import tech.energyit.state.model.Trade;

/**
 * NOTE: if there is only one writing thread the lock for {@link #add(Trade)} is biased.
 */
class TradeMemberIndex {

    private static final int INITIAL_CAPACITY = 32;
    private static final MutableLongList EMPTY_LIST = new LongArrayList();

    private Map<String, MutableLongList> tradeIdByMember = new ConcurrentHashMap<>();

    synchronized void add(Trade trade) {
        tradeIdByMember.computeIfAbsent(trade.getBuy().getMbrId(), initialLongList()).add(trade.getTradeId());
        tradeIdByMember.computeIfAbsent(trade.getSell().getMbrId(), initialLongList()).add(trade.getTradeId());
    }

    @NotNull
    private Function<String, MutableLongList> initialLongList() {
        return mbrId -> new LongArrayList(INITIAL_CAPACITY);
    }

    ImmutableLongList tradeIdsFor(String mbrId) {
        return tradeIdByMember.getOrDefault(mbrId, EMPTY_LIST).toImmutable();
    }
}
