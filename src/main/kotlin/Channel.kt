import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import kotlin.math.ceil
import kotlin.random.Random
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.nanoseconds

class P2PChannel(
    val socket: DatagramSocket,
    val peerEncryptor: HybridEncrypt,
    val myDecryptor: HybridDecrypt
) {
    var remoteIp = ""
    var remotePort = 0
    var myIp = ""
    var myPort = 0
    val sendedData = mutableMapOf<Int, MutableMap<Int, ByteArray>>()
    private val maxBytesInPacket = 1020

    suspend fun send(rawData: ByteArray) = withContext(Dispatchers.IO) {
        val dataSize = rawData.size
        val ip = myIp.split(".")

        if (dataSize + 20 > maxBytesInPacket) {
            try {
                val packetCount = ceil(dataSize.toFloat() / (maxBytesInPacket.toFloat() - 20f)).toInt()
                val random = Random.nextInt(1, 256)
                for (i in 0 until packetCount) {
                    val size = minOf(maxBytesInPacket - 20, (dataSize - (maxBytesInPacket - 20) * i)) + 20
                    val buffer = ByteArray(size)
                    buffer[0] = (i shr 24).toByte()
                    buffer[1] = (i shr 16).toByte()
                    buffer[2] = (i shr 8).toByte()
                    buffer[3] = i.toByte()
                    buffer[4] = random.toByte()
                    buffer[5] = (packetCount shr 24).toByte()
                    buffer[6] = (packetCount shr 16).toByte()
                    buffer[7] = (packetCount shr 8).toByte()
                    buffer[8] = packetCount.toByte()
                    buffer[9] = (size shr 8).toByte()
                    buffer[10] = size.toByte()
                    buffer[11] = ip[0].toInt().toByte()
                    buffer[12] = ip[1].toInt().toByte()
                    buffer[13] = ip[2].toInt().toByte()
                    buffer[14] = ip[3].toInt().toByte()
                    buffer[15] = (myPort shr 8).toByte()
                    buffer[16] = myPort.toByte()
                    System.arraycopy(rawData, (maxBytesInPacket - 20) * i, buffer, 20, size - 20)
                    sendedData.getOrPut(random) { mutableMapOf() }[i] = buffer
                    socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
                    Logger.logger.log(i.toString())
                    delay(1.microseconds)
                }
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        } else if (dataSize == 0) {
            try {
                val buffer = ByteArray(20)
                buffer[0] = (0 shr 24).toByte()
                buffer[1] = (0 shr 16).toByte()
                buffer[2] = (0 shr 8).toByte()
                buffer[3] = 0.toByte()
                buffer[4] = 0x00
                buffer[5] = (1 shr 24).toByte()
                buffer[6] = (1 shr 16).toByte()
                buffer[7] = (1 shr 8).toByte()
                buffer[8] = 1.toByte()
                buffer[9] = (20 shr 8).toByte()
                buffer[10] = 20.toByte()
                buffer[11] = ip[0].toInt().toByte()
                buffer[12] = ip[1].toInt().toByte()
                buffer[13] = ip[2].toInt().toByte()
                buffer[14] = ip[3].toInt().toByte()
                buffer[15] = (myPort shr 8).toByte()
                buffer[16] = myPort.toByte()
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        } else {
            try {
                val buffer = ByteArray(dataSize + 20)
                buffer[0] = (0 shr 24).toByte()
                buffer[1] = (0 shr 16).toByte()
                buffer[2] = (0 shr 8).toByte()
                buffer[3] = 0.toByte()
                buffer[4] = 0x00
                buffer[5] = (1 shr 24).toByte()
                buffer[6] = (1 shr 16).toByte()
                buffer[7] = (1 shr 8).toByte()
                buffer[8] = 1.toByte()
                buffer[9] = (buffer.size shr 8).toByte()
                buffer[10] = (buffer.size).toByte()
                buffer[11] = ip[0].toInt().toByte()
                buffer[12] = ip[1].toInt().toByte()
                buffer[13] = ip[2].toInt().toByte()
                buffer[14] = ip[3].toInt().toByte()
                buffer[15] = (myPort shr 8).toByte()
                buffer[16] = myPort.toByte()
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        }
    }

    suspend fun receive(): ByteArray {
        while (true) {
            val message = internalReceive()
            if (message.size < 20) {
                continue
            }
            val ip1 = message[11].toInt() and 0xff
            val ip2 = message[12].toInt() and 0xff
            val ip3 = message[13].toInt() and 0xff
            val ip4 = message[14].toInt() and 0xff
            val ip = "$ip1.$ip2.$ip3.$ip4"
            val port =
                ((message[15].toInt() and 0xFF) shl 8) or (message[16].toInt() and 0xFF)
            remotePort = port
            if (message.isEmpty()) {
                continue
            }
            if (ip == remoteIp) {
                val encryptedPayload = message.copyOfRange(20, message.size)
                if (encryptedPayload.isEmpty()) continue
                try {
                    //val decryptedBytes = myDecryptor.decrypt(encryptedPayload, null)
                    return encryptedPayload
                } catch (e: Exception) {
                    Logger.logger.log(e.message ?: "")
                    continue
                }
            }
        }


    }

    internal suspend fun internalReceive(): ByteArray = withContext(Dispatchers.IO) {
        val buffer = ByteArray(maxBytesInPacket)
        val packet = DatagramPacket(buffer, buffer.size)
        socket.receive(packet)
        if (buffer[0] == 0xFF.toByte() && packet.length == 2) {
            val id = buffer[1].toInt() and 0xff
            sendedData.remove(id)
            return@withContext ByteArray(0)
        }

        if (packet.length == 5) {
            val currentId = buffer[0].toInt() and 0xFF
            val currentNum = ((buffer[1].toInt() and 0xFF) shl 24) or
                    ((buffer[2].toInt() and 0xFF) shl 16) or
                    ((buffer[3].toInt() and 0xFF) shl 8) or
                    (buffer[4].toInt() and 0xFF)
            val data = sendedData[currentId]!![currentNum]
            socket.send(
                DatagramPacket(data, data!!.size, InetAddress.getByName(remoteIp), remotePort)
            )
            return@withContext ByteArray(0)
        }
        val count = ((buffer[5].toInt() and 0xFF) shl 24) or
                ((buffer[6].toInt() and 0xFF) shl 16) or
                ((buffer[7].toInt() and 0xFF) shl 8) or
                (buffer[8].toInt() and 0xFF)

        val id = buffer[4].toInt() and 0xff
        val size = ((buffer[9].toInt() and 0xFF) shl 8) or (buffer[10].toInt() and 0xFF)
        val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                ((buffer[1].toInt() and 0xFF) shl 16) or
                ((buffer[2].toInt() and 0xFF) shl 8) or
                (buffer[3].toInt() and 0xFF)

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
            val receivedNum = ByteArray(count)
            while (received < count) {
                try {
                    withTimeout(1000) {
                        socket.receive(DatagramPacket(buffer, buffer.size))
                    }
                    if (id == buffer[4].toInt() and 0xff) {
                        val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                                ((buffer[1].toInt() and 0xFF) shl 16) or
                                ((buffer[2].toInt() and 0xFF) shl 8) or
                                (buffer[3].toInt() and 0xFF)
                        receivedNum[num] = 1.toByte()
                        val currentSize = ((buffer[9].toInt() and 0xFF) shl 8) or (buffer[10].toInt() and 0xFF)
                        totalSize += (currentSize - 20)
                        System.arraycopy(
                            buffer,
                            20,
                            massiveBuffer,
                            20 + (maxBytesInPacket - 20) * num,
                            currentSize - 20
                        )
                        received++
                        Logger.logger.log(received.toString())
                    } else {
                        continue
                    }
                } catch (_: TimeoutCancellationException) {
                    launch {
                        receivedNum[id] = 1.toByte()
                        receivedNum.indices.filter { receivedNum[it] == 0.toByte() }.forEach {
                            socket.send(
                                DatagramPacket(
                                    byteArrayOf(
                                        id.toByte(), (it shr 24).toByte(),
                                        (it shr 16).toByte(),
                                        (it shr 8).toByte(),
                                        it.toByte()
                                    ), 5, InetAddress.getByName(remoteIp), remotePort
                                )
                            )
                        }
                    }
                    launch {
                        while (received < count) {
                            socket.receive(DatagramPacket(buffer, buffer.size))
                            if (id == buffer[4].toInt() and 0xff) {
                                val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                                        ((buffer[1].toInt() and 0xFF) shl 16) or
                                        ((buffer[2].toInt() and 0xFF) shl 8) or
                                        (buffer[3].toInt() and 0xFF)
                                receivedNum[num] = 1.toByte()
                                val currentSize = ((buffer[9].toInt() and 0xFF) shl 8) or (buffer[10].toInt() and 0xFF)
                                totalSize += (currentSize - 20)
                                System.arraycopy(
                                    buffer,
                                    20,
                                    massiveBuffer,
                                    20 + (maxBytesInPacket - 20) * num,
                                    currentSize - 20
                                )
                                received++
                                Logger.logger.log("$num ffffff")
                            } else {
                                continue
                            }
                        }
                    }
                }
            }
            socket.send(
                DatagramPacket(
                    byteArrayOf(0xFF.toByte(), id.toByte()),
                    2,
                    InetAddress.getByName(remoteIp),
                    remotePort
                )
            )
            Logger.logger.log("receive $totalSize bytes gg vp gg vp")
            return@withContext massiveBuffer.copyOfRange(0, totalSize)

        } else {
            Logger.logger.log("receive $size bytes")
            return@withContext buffer.copyOfRange(0, size)
        }
    }

    fun closeChannel() {
        socket.close()
    }
}