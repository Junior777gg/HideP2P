import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress

object StunManager {
    var stunAdress = "stun.cloudflare.com:3478"
    fun getAddress(socket: DatagramSocket): String? {
        val title = ByteArray(20)
        title[0] = 0x00; title[1] = 0x01; title[2] = 0x00; title[3] = 0x00
        title[4] = 0x21; title[5] = 0x12; title[6] = 0xA4.toByte(); title[7] = 0x42
        socket.soTimeout = 3000
        socket.send(
            DatagramPacket(
                title, title.size,
                InetAddress.getByName(stunAdress.split(":")[0]), stunAdress.split(":")[1].toInt(),
            )
        )
        try {
            val response = ByteArray(256)
            val packet = DatagramPacket(response, response.size)
            socket.receive(packet)
            var i = 20
            while (i < packet.length) {
                val type = ((response[i].toInt() and 0xff) shl 8) or (response[i + 1].toInt() and 0xff)
                val length = ((response[i + 2].toInt() and 0xff) shl 8) or (response[i + 3].toInt() and 0xff)
                if (type == 0x0020) {
                    val port =
                        (((response[i + 6].toInt() and 0xff) shl 8) or (response[i + 7].toInt() and 0xff)) xor 0x2112
                    val ip =
                        "${(response[i + 8].toInt() and 0xff) xor 0x21}.${(response[i + 9].toInt() and 0xff) xor 0x12}." +
                                "${(response[i + 10].toInt() and 0xff) xor 0xA4}.${(response[i + 11].toInt() and 0xff) xor 0x42}"
                    return "$ip:$port"
                }
                i += 4 + length
            }
            return null
        }catch (e:Exception){
            e.printStackTrace()
        }
        return null
    }
}