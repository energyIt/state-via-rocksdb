package model

import net.openhft.chronicle.wire.AbstractBytesMarshallable

public class HalfTrade(
        val dlvryAreaId: String,
        val acctId: String,
        val orderId: Long,
        val txt: String? = null,
        val userCode: String,
        val mbrId: String) : AbstractBytesMarshallable() {


}
