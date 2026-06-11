import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import kotlin.random.Random

class P2PChannel(val socket: DatagramSocket) {
    var ip = ""
    var port = 0

    suspend fun send(message: UDPFormat) = withContext(Dispatchers.IO) {
        val serializedMessage = Json.encodeToString(message)
        val messageBytes = serializedMessage.toByteArray()
        val currentBytes = ByteArray(256)
        var packetCount = 0

        if (messageBytes.size > currentBytes.size - 20) {
            packetCount = (messageBytes.size / (currentBytes.size - 20)) + 1
            val buffer = ByteArray(256)
            var last = 0
            val random = Random.nextInt(256)

            for (i in 1..packetCount) {
                val to = minOf(last + 236, messageBytes.size)
                System.arraycopy(
                    messageBytes.copyOfRange(last, to), 0,
                    buffer, 20,
                    to - last
                )
                last += currentBytes.size - 20
                buffer[0] = i.toByte()
                buffer[1] = random.toByte()
                buffer[2] = packetCount.toByte()

                if (i == packetCount) {
                    buffer[3] = (messageBytes.size - 236 * (packetCount - 1)).toByte()
                } else {
                    buffer[3] = 236.toByte()
                }

                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(ip), port))
            }

        } else {
            val random = Random.nextInt()
            packetCount = 1
            System.arraycopy(messageBytes, 0, currentBytes, 20, messageBytes.size)
            currentBytes[0] = 0x01
            currentBytes[1] = random.toByte()
            currentBytes[2] = packetCount.toByte()
            currentBytes[3] = messageBytes.size.toByte()
            socket.send(DatagramPacket(currentBytes, currentBytes.size, InetAddress.getByName(ip), port))
        }

    }

    suspend fun receive(): UDPFormat {
        while (true) {
           val message = internalReceive(null)
            if (message.type != "ping" && message.type != "connect" && message.type != "keep") {
                return message
            }
        }
    }

    private suspend fun internalReceive(message: ByteArray?): UDPFormat = withContext(Dispatchers.IO) {
            val buffer = ByteArray(256)
            if (message == null) {
                socket.receive(DatagramPacket(buffer, buffer.size))
            } else {
                message.copyInto(buffer)
            }

            val count = buffer[2].toInt()
            val id = buffer[1].toInt()
            val size = buffer[3].toInt()
            if (count > 1) {
                var currentMessage = Array(count) { "" }
                currentMessage[buffer[0].toInt() - 1] = String(buffer.copyOfRange(20, 20 + size))
                var received = 1

                while (received < count) {
                    socket.receive(DatagramPacket(buffer, buffer.size))

                    if (id == buffer[1].toInt()) {
                        currentMessage[buffer[0].toInt() - 1] = String(buffer.copyOfRange(20, buffer[3].toInt() + 20))
                        received++
                    } else {
                        continue
                    }
                }
                var stringMessage = ""
                currentMessage.forEach { piece ->
                    stringMessage += piece
                }
                return@withContext Json.decodeFromString<UDPFormat>(stringMessage)

            } else {
                val message = Json.decodeFromString<UDPFormat>(String(buffer.copyOfRange(20, buffer[3].toInt() + 20)))
                return@withContext message
            }
    }
}