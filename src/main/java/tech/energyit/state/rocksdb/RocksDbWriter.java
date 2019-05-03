package tech.energyit.state.rocksdb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.function.ToLongFunction;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import tech.energyit.state.repository.Writer;

public class RocksDbWriter implements Closeable, Writer {

    private static ThreadLocal<Bytes<ByteBuffer>> keyBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(8));
    private static ThreadLocal<Bytes<ByteBuffer>> valueBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(128));

    private final RocksDB db;
    private final WireType wireType;
    private final FlushOptions flushOptions = new FlushOptions()
            .setWaitForFlush(true);
    private final WriteOptions writeOptions = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false)
            .setIgnoreMissingColumnFamilies(false)
            .setNoSlowdown(false);

    public RocksDbWriter(RocksDB db, WireType wireType) {
        this.db = db;
        this.wireType = wireType;
    }

    public void put(long key, Object entity) {
        try {
            db.put(writeOptions, keyAsBytes(key), valueAsBytes(entity));
        } catch (RocksDBException e) {
            throw new IllegalArgumentException("Write failed", e);
        }
    }

    /**
     * Atomically writes all or nothing
     */
    public <T> void putAll(ToLongFunction<T> keySupplier, Iterable<T> entities) {
        try (WriteBatch writeBatch = new WriteBatch()) {
            for (T e : entities) {
                writeBatch.put(keyAsBytes(keySupplier.applyAsLong(e)), valueAsBytes(e));
            }
            db.write(writeOptions,writeBatch);
        } catch (RocksDBException e) {
            throw new IllegalArgumentException("Write failed", e);
        }
    }

    private byte[] valueAsBytes(Object entity) {
        Wire wire = wireType.apply(valueBytes.get().clear());
        wire.write().object(entity);
        return wire.bytes().toByteArray();
    }

    private byte[] keyAsBytes(long id) {
        return keyBytes.get().clear().writeLong(id).toByteArray();
    }

    public long lastSequence() {
        return db.getLatestSequenceNumber();
    }

    public void flush() {
        try {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new IllegalArgumentException("Flush failed", e);
        }
    }

    @Override
    public void close() {
        flushOptions.close();
        writeOptions.close();
    }

}
