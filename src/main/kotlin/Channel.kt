import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import kotlin.math.ceil
import kotlin.random.Random

class P2PChannel(
    val socket: DatagramSocket,
    val peerEncryptor: HybridEncrypt,
    val myDecryptor: HybridDecrypt
) {
    var remoteIp = ""
    var remotePort = 0
    var myIp = ""
    var myPort = 0

    private val maxBytesInPacket = 1020

    suspend fun send(rawData: ByteArray) = withContext(Dispatchers.IO) {
        val data = if (rawData.isEmpty()) {
            rawData
        } else {
            peerEncryptor.encrypt(rawData, null)
        }
        val dataSize = data.size
        val ip = myIp.split(".")

        if (dataSize + 20 > maxBytesInPacket) {
            try {
                val packetCount = ceil(dataSize.toFloat() / (maxBytesInPacket.toFloat() - 20f)).toInt()
                val random = Random.nextInt(1, 256)
                for (i in 0 until packetCount) {
                    val size = minOf(maxBytesInPacket - 20, (dataSize - (maxBytesInPacket - 20) * i)) + 20
                    val buffer = ByteArray(size)
                    buffer[0] = i.toByte()
                    buffer[1] = random.toByte()
                    buffer[2] = (packetCount shr 8).toByte()
                    buffer[3] = packetCount.toByte()
                    buffer[4] = (size shr 8).toByte()
                    buffer[5] = size.toByte()
                    buffer[6] = ip[0].toInt().toByte()
                    buffer[7] = ip[1].toInt().toByte()
                    buffer[8] = ip[2].toInt().toByte()
                    buffer[9] = ip[3].toInt().toByte()
                    buffer[10] = (myPort shr 8).toByte()
                    buffer[11] = myPort.toByte()
                    System.arraycopy(data, (maxBytesInPacket - 20) * i, buffer, 20, size - 20)
                    socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
                    Logger.logger.log(size.toString())
                }
            } catch (e: Exception) {
                Logger.logger.log(e.message?:"")
            }
        }else if (dataSize == 0) {
            try {
                val buffer = ByteArray(20)
                buffer[0] = 0x00
                buffer[1] = 0x00
                buffer[2] = (1 shr 8).toByte()
                buffer[3] = 1.toByte()
                buffer[4] = (20 shr 8).toByte()
                buffer[5] = 20.toByte()
                buffer[6] = ip[0].toInt().toByte()
                buffer[7] = ip[1].toInt().toByte()
                buffer[8] = ip[2].toInt().toByte()
                buffer[9] = ip[3].toInt().toByte()
                buffer[10] = (myPort shr 8).toByte()
                buffer[11] = myPort.toByte()
                System.arraycopy(data, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
            } catch (e: Exception) {
                Logger.logger.log(e.message?:"")
            }
        }else
         {
            try {
                val buffer = ByteArray(dataSize + 20)
                buffer[0]= 0x00
                buffer[1] = 0x00
                buffer[2] = (1 shr 8).toByte()
                buffer[3] = 1.toByte()
                buffer[4] = (buffer.size shr 8).toByte()
                buffer[5] = buffer.size.toByte()
                buffer[6] = ip[0].toInt().toByte()
                buffer[7] = ip[1].toInt().toByte()
                buffer[8] = ip[2].toInt().toByte()
                buffer[9] = ip[3].toInt().toByte()
                buffer[10] = (myPort shr 8).toByte()
                buffer[11] = myPort.toByte()
                System.arraycopy(data, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
            } catch (e: Exception) {
                Logger.logger.log(e.message?:"")
            }
        }
    }

    suspend fun receive(): ByteArray {
        while (true) {
            val message = internalReceive()
            if (message.size < 20){
                continue
            }
            val ip1 = message[6].toInt() and 0xff
            val ip2 = message[7].toInt() and 0xff
            val ip3 = message[8].toInt() and 0xff
            val ip4 = message[9].toInt() and 0xff
            val ip = "$ip1.$ip2.$ip3.$ip4"
            val port =
                ((message[10].toInt() and 0xFF) shl 8) or (message[11].toInt() and 0xFF)
            remotePort = port
            if (message.isEmpty()) {
                continue
            }
            if (ip == remoteIp) {
                val encryptedPayload = message.copyOfRange(20, message.size)
                if (encryptedPayload.isEmpty()) continue
                try {
                    val decryptedBytes = myDecryptor.decrypt(encryptedPayload, null)
                    return decryptedBytes
                } catch (e: Exception) {
                    Logger.logger.log(e.message?:"")
                    continue
                }
            }
        }


    }

    internal suspend fun internalReceive(): ByteArray = withContext(Dispatchers.IO) {
        val buffer = ByteArray(maxBytesInPacket)
        val packet = DatagramPacket(buffer, buffer.size)
        socket.receive(packet)
        val count = ((buffer[2].toInt() and 0xFF) shl 8) or (buffer[3].toInt() and 0xFF)
        val id = buffer[1].toInt() and 0xff
        val size = ((buffer[4].toInt() and 0xFF) shl 8) or (buffer[5].toInt() and 0xFF)
        val num = buffer[0].toInt() and 0xFF
        var totalSize = size
        if (count > 1) {
            var received = 1
            val massiveBuffer = ByteArray(count * maxBytesInPacket)
            System.arraycopy(buffer, 0, massiveBuffer, 0, 20)
            System.arraycopy(
                buffer,
                20,
                massiveBuffer,
                20 + (maxBytesInPacket - 20) * num,
                size - 20
            )
            while (received < count) {
                Logger.logger.log("фыфыфыфыфыфыфыфыфыфыфыфыфыф")
                socket.receive(DatagramPacket(buffer, buffer.size))
                if ((id == buffer[1].toInt() and 0xff) and (count ==((buffer[2].toInt() and 0xFF) shl 8) or (buffer[3].toInt() and 0xFF))) {
                    val num = buffer[0].toInt() and 0xff
                    val currentSize = ((buffer[4].toInt() and 0xFF) shl 8) or (buffer[5].toInt() and 0xFF)
                    totalSize += (currentSize - 20)
                    System.arraycopy(
                        buffer,
                        20,
                        massiveBuffer,
                        20 + (maxBytesInPacket - 20) * num,
                        currentSize - 20
                    )
                    received++
                } else {
                    continue
                }
            }
            Logger.logger.log("receive $totalSize bytes gg vp gg vp")
            return@withContext massiveBuffer.copyOfRange(0, totalSize)

        } else {
            Logger.logger.log("receive $size bytes")
            return@withContext buffer.copyOfRange(0, size)
        }
    }

    fun closeChannel(){
        socket.close()
    }
}