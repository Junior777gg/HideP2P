import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import kotlin.random.Random

object StunManager {
    var stunAddress = "stun.cloudflare.com:3478"
    fun getLocalAddress(isIpv6: Boolean): InetSocketAddress? {
        val socket = DatagramSocket()
        socket.connect(InetAddress.getByName("8.8.8.8"), 53)
        val interf = NetworkInterface.getByInetAddress(socket.localAddress)
        socket.close()
        interf.inetAddresses.toList().forEach { addr ->
            if (!addr.isLinkLocalAddress && !addr.isLoopbackAddress){
                if (isIpv6 && addr is Inet6Address) {
                    return InetSocketAddress(addr, Random.nextInt(1024,65536))
                }else if (!isIpv6 && addr is Inet4Address){
                    return InetSocketAddress(addr, Random.nextInt(1024,65536))
                }
            }
        }
        return null
    }

    fun getAddress(socket: DatagramSocket, isIpv6: Boolean): InetSocketAddress? {
        val targetAddress = if (isIpv6) {
            InetAddress.getAllByName(stunAddress.substringBefore(":")).firstOrNull { it is Inet6Address }
        } else InetAddress.getAllByName(stunAddress.substringBefore(":")).firstOrNull { it is Inet4Address }
        val title = ByteArray(20)
        title[0] = 0x00; title[1] = 0x01; title[2] = 0x00; title[3] = 0x00
        title[4] = 0x21; title[5] = 0x12; title[6] = 0xA4.toByte(); title[7] = 0x42
        socket.soTimeout = 3000
        try {
            socket.send(
                DatagramPacket(
                    title, title.size,
                    targetAddress, stunAddress.substringAfter(":").toInt(),
                )
            )
        }catch (_: Exception){
            return null
        }
        try {
            val response = ByteArray(512)
            val packet = DatagramPacket(response, response.size)
            socket.receive(packet)
            var i = 20
            while (i < packet.length) {
                val type = ((response[i].toInt() and 0xff) shl 8) or (response[i + 1].toInt() and 0xff)
                val length = ((response[i + 2].toInt() and 0xff) shl 8) or (response[i + 3].toInt() and 0xff)
                if (type == 0x0020) {
                    val xorKeys = byteArrayOf(0x21, 0x12, 0xA4.toByte(), 0x42) + ByteArray(12)
                    val port =
                        (((response[i + 6].toInt() and 0xff) shl 8) or (response[i + 7].toInt() and 0xff)) xor 0x2112
                    if (isIpv6) {
                        val ip = ByteArray(16)
                        for (k in 0 until 16) {
                            ip[k] = ((response[i + 8 + k].toInt() and 0xff) xor xorKeys[k].toInt()).toByte()
                        }
                        return InetSocketAddress(Inet6Address.getByAddress(ip), port)
                    } else {
                        val ip = ByteArray(4)
                        for (k in 0 until 4) {
                            ip[k] = ((response[i + 8 + k].toInt() and 0xff) xor xorKeys[k].toInt()).toByte()
                        }
                        return InetSocketAddress(Inet4Address.getByAddress(ip), port)
                    }
                }
                i += 4 + length
            }
            return null
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return null
    }
}