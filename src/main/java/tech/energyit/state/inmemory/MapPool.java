package tech.energyit.state.inmemory;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
import java.util.function.ToLongFunction;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import tech.energyit.state.repository.Reader;
import tech.energyit.state.repository.Writer;

/**
 * Not Thread safe - no concurrent access possible.
 *
 * @author gregmil
 */
public class MapPool implements Reader, Writer {

    private MutableLongObjectMap<Object> idIndex = LongObjectHashMap.newMap();

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
