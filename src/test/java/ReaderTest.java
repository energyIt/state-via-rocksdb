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
        Assertions.assertThat(Reader.asLong(Reader.asByteArray(id))).isEqualTo(id);
    }
}
