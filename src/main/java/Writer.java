import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Writer  {

    private static Logger LOG = LoggerFactory.getLogger(Writer.class);

    private static ThreadLocal<Bytes<ByteBuffer>> keyBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(8));
    private static ThreadLocal<Bytes<ByteBuffer>> valueBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(128));

    private final RocksDB db;
    private final WireType wireType;

    public Writer(RocksDB db, WireType wireType) {
        this.db = db;
        this.wireType = wireType;
    }

    public void put(long key, Object entity) {
       Wire wire = wireType.apply( valueBytes.get().clear());
        wire.write().object(entity);
        try {
            db.put(asByteArray(key), wire.bytes().toByteArray());
            LOG.trace("written : {}", wire);
        } catch (RocksDBException e) {
            throw new IllegalArgumentException("Write failed", e);
        }
    }

    public long lastSequence() {
        return db.getLatestSequenceNumber();
    }

    private byte[] asByteArray(long id) {
        return keyBytes.get().clear().writeLong(id).toByteArray();
    }
}
