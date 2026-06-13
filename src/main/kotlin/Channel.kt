import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import kotlin.experimental.xor
import kotlin.random.Random

class P2PChannel(val socket: DatagramSocket) {
    var remoteIp = ""
    var remotePort = 0
    var myIp = ""
    var myPort = 0

    private val maxBytesInPacket = 1020

    suspend fun send(data: ByteArray) = withContext(Dispatchers.IO) {
        val dataSize = data.size

        if (dataSize + 20 > maxBytesInPacket) {
            val packetCount = dataSize / (maxBytesInPacket - 20) + 1
            val random = Random.nextInt(256)
            for (i in 1..packetCount) {
                val size = minOf(maxBytesInPacket - 20, (dataSize - (maxBytesInPacket - 20) * (i - 1))) + 20
                val buffer = ByteArray(size)
                buffer[0] = 0x40
                buffer[1] = Random.nextInt(256).toByte()
                buffer[2] = Random.nextInt(256).toByte()
                buffer[3] = Random.nextInt(256).toByte()
                buffer[4] = Random.nextInt(256).toByte()
                buffer[5] = i.toByte()
                buffer[6] = random.toByte()
                buffer[7] = packetCount.toByte()
                buffer[8] = (size shr 8).toByte()
                buffer[9] = size.toByte()
                val ip = myIp.split(".")
                buffer[10] = ip[0].toByte() xor buffer[1]
                buffer[11] = ip[1].toByte() xor buffer[2]
                buffer[12] = ip[2].toByte() xor buffer[3]
                buffer[13] = ip[3].toByte() xor buffer[4]
                buffer[14] = (myPort shr 8).toByte() xor buffer[1]
                buffer[15] = myPort.toByte() xor buffer[2]
                System.arraycopy(data, (maxBytesInPacket - 20) * (i - 1), buffer, 20, size - 20)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
            }
        } else {
            val buffer = ByteArray(dataSize + 20)
            buffer[0] = 0x40
            buffer[1] = Random.nextInt(256).toByte()
            buffer[2] = Random.nextInt(256).toByte()
            buffer[3] = Random.nextInt(256).toByte()
            buffer[4] = Random.nextInt(256).toByte()
            buffer[5] = 0x01
            buffer[6] = Random.nextInt(256).toByte()
            buffer[7] = 0x01
            buffer[8] = (buffer.size shr 8).toByte()
            buffer[9] = buffer.size.toByte()
            val ip = myIp.split(".")
            buffer[10] = ip[0].toByte() xor buffer[1]
            buffer[11] = ip[1].toByte() xor buffer[2]
            buffer[12] = ip[2].toByte() xor buffer[3]
            buffer[13] = ip[3].toByte() xor buffer[4]
            buffer[14] = (myPort shr 8).toByte() xor buffer[1]
            buffer[15] = myPort.toByte() xor buffer[2]
            System.arraycopy(data, 0, buffer, 20, dataSize)
            socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
        }
    }

    suspend fun receive(): ByteArray {
        while (true) {
            val message = internalReceive()
            val ip1 = (message[10] xor message[1]).toInt() and 0xff
            val ip2 = (message[11] xor message[2]).toInt() and 0xff
            val ip3 = (message[12] xor message[3]).toInt() and 0xff
            val ip4 = (message[13] xor message[4]).toInt() and 0xff
            val ip = "$ip1.$ip2.$ip3.$ip4"
            val port = (((message[14] xor message[1]).toInt() and 0xFF) shl 8) or ((message[15] xor message[2]).toInt() and 0xFF)
            val size = ((message[8].toInt() and 0xFF) shl 8) or (message[9].toInt() and 0xFF)
            if (ip == remoteIp && port == remotePort && size > 20) {
                return message.copyOfRange(20, message.size)
            }
        }
    }

    internal suspend fun internalReceive(): ByteArray = withContext(Dispatchers.IO) {
        val buffer = ByteArray(maxBytesInPacket)
        socket.receive(DatagramPacket(buffer, buffer.size))
        val count = buffer[7].toInt() and 0xff
        val id = buffer[6].toInt() and 0xff
        val size = ((buffer[8].toInt() and 0xFF) shl 8) or (buffer[9].toInt() and 0xFF)
        val firstNum = buffer[5].toInt() and 0xff
        var totalSize = size
        if (count > 1) {
            var received = 1
            val massiveBuffer = ByteArray(count * maxBytesInPacket)
            System.arraycopy(buffer, 0, massiveBuffer, 0, 20)
            System.arraycopy(
                buffer,
                20,
                massiveBuffer,
                20 + (maxBytesInPacket - 20) * (firstNum - 1),
                size - 20
            )
            while (received < count) {
                socket.receive(DatagramPacket(buffer, buffer.size))
                if (id == buffer[6].toInt() and 0xff) {
                    val num = buffer[5].toInt() and 0xff
                    val currentSize = ((buffer[8].toInt() and 0xFF) shl 8) or (buffer[9].toInt() and 0xFF)
                    totalSize += (currentSize - 20)
                    System.arraycopy(
                        buffer,
                        20,
                        massiveBuffer,
                        20 + (maxBytesInPacket - 20) * (num - 1),
                        currentSize - 20
                        )
                    received++
                } else {
                    continue
                }
            }
            return@withContext massiveBuffer.copyOfRange(0, totalSize)

        } else {
            return@withContext buffer.copyOfRange(0, size)
        }
    }
}