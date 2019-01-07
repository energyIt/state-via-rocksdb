package model

import net.openhft.chronicle.wire.AbstractBytesMarshallable
import java.time.LocalDateTime

public class Trade(
        val tradeId: Long,
        val state: String,
        val contractId: Long,
        val qty: Int,
        val price: Int,
        val execTime: LocalDateTime,
        val revisionNo: Long = 0,
        val buy: HalfTrade,
        val sell: HalfTrade
) : AbstractBytesMarshallable() {
}
