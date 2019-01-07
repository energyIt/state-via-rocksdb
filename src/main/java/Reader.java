import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;

public class Reader {

    private static Logger LOG = LoggerFactory.getLogger(Reader.class);

    private static ThreadLocal<Bytes<ByteBuffer>> keyBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(8));
    private static ThreadLocal<Bytes<ByteBuffer>> valueBytes = ThreadLocal.withInitial(() -> Bytes.elasticByteBuffer(128));


    private final RocksDB db;
    private final WireType wireType;

    public Reader(RocksDB db, WireType wireType) {
        this.db = db;
        this.wireType = wireType;
    }

    public <T> T get(long id, Class<T> clazz) {
        byte[] value;
        try {
            value = db.get(asByteArray(id));
        } catch (RocksDBException e) {
            throw new IllegalArgumentException("get failed", e);
        }
        if (value != null) {
            Wire wire = wireType.apply(valueBytes.get().clear().write(value));
            T object = wire.read().object(clazz);
            LOG.trace("Read : {}", object);
            return object;
        } else {
            return null;
        }
    }

    private byte[] asByteArray(long id) {
        return keyBytes.get().clear().writeLong(id).toByteArray();
    }

}
