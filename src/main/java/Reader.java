import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return convertValue(clazz, value);
    }

    private <T> T convertValue(Class<T> clazz, byte[] value) {
        if (value != null) {
            Wire wire = wireType.apply(valueBytes.get().clear().write(value));
            T object = wire.read().object(clazz);
            LOG.trace("Read : {}", object);
            return object;
        } else {
            return null;
        }
    }

    /**
     * @return all items with IDs between <code>idFrom</code> and <code>idTo</code>, both included.
     */
    public <T> List<T> getAllBetween(long idFrom, long idTo, Class<T> clazz) {
        try (RocksIterator iter = db.newIterator()) {
            final List<T> result = new ArrayList<>((int) (idTo - idFrom));
            for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                byte[] k = iter.key();
                long id = asLong(k);
                if (id >= idFrom && id <= idTo) {
                    byte[] v = iter.value();
                    result.add(convertValue(clazz, v));
                }
            }
            return result;
        }
    }

    static byte[] asByteArray(long id) {
        return keyBytes.get().clear().writeLong(id).toByteArray();
    }

    static long asLong(byte[] key) {
        return keyBytes.get().clear().write(key).readLong();
    }
}
