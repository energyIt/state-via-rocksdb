package tech.energyit.state.rocksdb;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *
 * @author gregmil
 */
public class ReaderTest {

    @Test
    public void serializedKeyMustBeDeserializedCorrectly() {
        final int id = 1000;
        Assertions.assertThat(RocksDbReader.asLong(RocksDbReader.asByteArray(id))).isEqualTo(id);
    }
}
