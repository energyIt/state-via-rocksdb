package tech.energyit.state.repository;

import java.util.function.ToLongFunction;

public interface Writer {

    void put(long key, Object entity);

    /**
     * Atomically writes all or nothing
     */
    <T> void putAll(ToLongFunction<T> keySupplier, Iterable<T> entities);

}
