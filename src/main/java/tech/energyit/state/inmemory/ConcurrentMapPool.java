package tech.energyit.state.inmemory;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.ToLongFunction;

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import tech.energyit.state.repository.Reader;
import tech.energyit.state.repository.Writer;

/**
 * Not all cases are thread safe !
 * @author gregmil
 */
public class ConcurrentMapPool implements Reader, Writer {

    private ConcurrentHashMap<Long, Object> idIndex = ConcurrentHashMap.newMap();

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(long id, Class<T> clazz) {
        return (T) idIndex.get(id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Collection<T> getAllBetween(long idFrom, long idTo, Class<T> clazz) {
        return (Collection<T>) idIndex.select((id, o) -> id >= idFrom && id <= idTo).values();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void forEach(LongPredicate idPredicate, Consumer<T> handler, Class<T> clazz) {
        idIndex.select((id, o) -> idPredicate.test(id)).forEach((Consumer) handler);
    }

    @Override
    public void put(long key, Object entity) {
        idIndex.put(key, entity);
    }

    @Override
    public <T> void putAll(ToLongFunction<T> keySupplier, Iterable<T> entities) {
        entities.forEach(v -> idIndex.put(keySupplier.applyAsLong(v), v));
    }
}
