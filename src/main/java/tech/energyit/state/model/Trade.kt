package tech.energyit.state.model

import net.openhft.chronicle.bytes.BytesMarshallable
import java.time.LocalDateTime

data class Trade(
    val tradeId: Long,
    val state: String,
    val contractId: Long,
    val qty: Int,
    val price: Int,
    val execTime: LocalDateTime,
    val revisionNo: Long = 0,
    val buy: HalfTrade,
    val sell: HalfTrade,
) : BytesMarshallable
