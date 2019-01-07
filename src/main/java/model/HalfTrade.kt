package model

import net.openhft.chronicle.wire.AbstractBytesMarshallable

public class HalfTrade(
        private val dlvryAreaId: String,
        private val acctId: String,
        private val orderId: Long,
        private val txt: String? = null,
        private val userCode: String,
        private val mbrId: String) : AbstractBytesMarshallable() {


}
