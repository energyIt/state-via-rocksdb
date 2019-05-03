package tech.energyit.state.repository;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.LongPredicate;

public interface Reader {

    <T> T get(long id, Class<T> clazz);

    /**
     * @return all items with IDs between <code>idFrom</code> and <code>idTo</code>, both included.
     */
    <T> Collection<T> getAllBetween(long idFrom, long idTo, Class<T> clazz);

    <T> void forEach(LongPredicate idPredicate, Consumer<T> handler, Class<T> clazz);

}
