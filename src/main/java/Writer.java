import java.io.Closeable;
import java.nio.ByteBuffer;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class Writer implements Closeable {

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

    public Writer(RocksDB db, WireType wireType) {
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
