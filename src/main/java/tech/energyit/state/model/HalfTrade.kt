package tech.energyit.state.model

import net.openhft.chronicle.bytes.BytesMarshallable

data class HalfTrade(
    val dlvryAreaId: String,
    val acctId: String,
    val orderId: Long,
    val txt: String? = null,
    val userCode: String,
    val mbrId: String,
) : BytesMarshallable
