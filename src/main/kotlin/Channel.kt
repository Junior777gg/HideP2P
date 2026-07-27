import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import kotlin.math.ceil
import kotlin.random.Random
import kotlin.time.Duration.Companion.microseconds

class P2PChannel(
    val socket: DatagramSocket,
    val peerEncryptor: HybridEncrypt,
    val myDecryptor: HybridDecrypt
) {
    init {
        CoroutineScope(Dispatchers.IO).launch {
            internalReceive()
        }
    }

    var remoteIp = ""
    var remotePort = 0
    var myIp = ""
    var myPort = 0
    internal var status = ""
    private val sendData = mutableMapOf<Int, MutableMap<Int, ByteArray>>()
    private val maxBytesInPacket = 1020
    private val messageReceiver = Channel<ByteArray>(Channel.UNLIMITED)

    suspend fun send(rawData: ByteArray) = withContext(Dispatchers.IO) {
        val dataSize = rawData.size
        val ip = myIp.split(".")

        if (dataSize + 20 > maxBytesInPacket) {
            try {
                val packetCount = ceil(dataSize.toFloat() / (maxBytesInPacket.toFloat() - 20f)).toInt()
                val random = Random.nextInt(65536)
                for (i in 0 until packetCount) {
                    val size = minOf(maxBytesInPacket - 20, (dataSize - (maxBytesInPacket - 20) * i)) + 20
                    val buffer = ByteArray(size)
                    buffer[0] = (i shr 24).toByte()
                    buffer[1] = (i shr 16).toByte()
                    buffer[2] = (i shr 8).toByte()
                    buffer[3] = i.toByte()
                    buffer[4] = (random shr 8).toByte()
                    buffer[5] = random.toByte()
                    buffer[6] = (packetCount shr 24).toByte()
                    buffer[7] = (packetCount shr 16).toByte()
                    buffer[8] = (packetCount shr 8).toByte()
                    buffer[9] = packetCount.toByte()
                    buffer[10] = (size shr 8).toByte()
                    buffer[11] = size.toByte()
                    buffer[12] = ip[0].toInt().toByte()
                    buffer[13] = ip[1].toInt().toByte()
                    buffer[14] = ip[2].toInt().toByte()
                    buffer[15] = ip[3].toInt().toByte()
                    buffer[16] = (myPort shr 8).toByte()
                    buffer[17] = myPort.toByte()
                    System.arraycopy(rawData, (maxBytesInPacket - 20) * i, buffer, 20, size - 20)
                    sendData.getOrPut(random) { mutableMapOf() }[i] = buffer
                    socket.send(DatagramPacket(buffer, size, InetAddress.getByName(remoteIp), remotePort))
                    Logger.logger.log(i.toString())
                }
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        } else if (dataSize == 0) {
            try {
                val random = Random.nextInt(65536)
                val buffer = ByteArray(20)
                buffer[0] = (0 shr 24).toByte()
                buffer[1] = (0 shr 16).toByte()
                buffer[2] = (0 shr 8).toByte()
                buffer[3] = 0.toByte()
                buffer[4] = (random shr 8).toByte()
                buffer[5] = random.toByte()
                buffer[6] = (1 shr 24).toByte()
                buffer[7] = (1 shr 16).toByte()
                buffer[8] = (1 shr 8).toByte()
                buffer[9] = 1.toByte()
                buffer[10] = (20 shr 8).toByte()
                buffer[11] = 20.toByte()
                buffer[12] = ip[0].toInt().toByte()
                buffer[13] = ip[1].toInt().toByte()
                buffer[14] = ip[2].toInt().toByte()
                buffer[15] = ip[3].toInt().toByte()
                buffer[16] = (myPort shr 8).toByte()
                buffer[17] = myPort.toByte()
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, 20, InetAddress.getByName(remoteIp), remotePort))
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        } else {
            try {
                val random = Random.nextInt(65536)
                val buffer = ByteArray(dataSize + 20)
                buffer[0] = (0 shr 24).toByte()
                buffer[1] = (0 shr 16).toByte()
                buffer[2] = (0 shr 8).toByte()
                buffer[3] = 0.toByte()
                buffer[4] = (random shr 8).toByte()
                buffer[5] = random.toByte()
                buffer[6] = (1 shr 24).toByte()
                buffer[7] = (1 shr 16).toByte()
                buffer[8] = (1 shr 8).toByte()
                buffer[9] = 1.toByte()
                buffer[10] = (buffer.size shr 8).toByte()
                buffer[11] = (buffer.size).toByte()
                buffer[12] = ip[0].toInt().toByte()
                buffer[13] = ip[1].toInt().toByte()
                buffer[14] = ip[2].toInt().toByte()
                buffer[15] = ip[3].toInt().toByte()
                buffer[16] = (myPort shr 8).toByte()
                buffer[17] = myPort.toByte()
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, InetAddress.getByName(remoteIp), remotePort))
                Logger.logger.log("fffffffffffffffffffffff")
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "")
            }
        }
    }

    suspend fun receive(): ByteArray {
        while (true) {
            val message = messageReceiver.receive()
            if (message.size < 20) {
                continue
            }
            val ip1 = message[12].toInt() and 0xff
            val ip2 = message[13].toInt() and 0xff
            val ip3 = message[14].toInt() and 0xff
            val ip4 = message[15].toInt() and 0xff
            val ip = "$ip1.$ip2.$ip3.$ip4"
            val port =
                ((message[16].toInt() and 0xFF) shl 8) or (message[17].toInt() and 0xFF)
            remotePort = port
            if (message.isEmpty()) {
                continue
            }
            if (ip == remoteIp) {
                val encryptedPayload = message.copyOfRange(20, message.size)
                if (encryptedPayload.isEmpty()) continue
                try {
                    //val decryptedBytes = myDecryptor.decrypt(encryptedPayload, null)
                    Logger.logger.log("ZZZZZZZZZZZZZZ")
                    return encryptedPayload
                } catch (e: Exception) {
                    Logger.logger.log(e.message ?: "")
                    continue
                }
            } else {
                continue

            }
        }
    }

    internal suspend fun internalReceive(): Unit = withContext(Dispatchers.IO) {
        val mapOfBytes = mutableMapOf<Int, Channel<ByteArray>>()
        val flowOfIds = MutableSharedFlow<Int>(5)
        launch {
            while (true) {
                val buffer = ByteArray(maxBytesInPacket)
                val packet = DatagramPacket(buffer, buffer.size)
                socket.receive(packet)
                Logger.logger.log("gordon")
                status = "new_packet"
                if (packet.length == 6) {
                    val currentId = ((buffer[0].toInt() and 0xFF) shl 8) or (buffer[1].toInt() and 0xFF)
                    val currentNum = ((buffer[2].toInt() and 0xFF) shl 24) or
                            ((buffer[3].toInt() and 0xFF) shl 16) or
                            ((buffer[4].toInt() and 0xFF) shl 8) or
                            (buffer[5].toInt() and 0xFF)
                    val data = sendData[currentId]!![currentNum]
                    socket.send(
                        DatagramPacket(data, data!!.size, InetAddress.getByName(remoteIp), remotePort)
                    )
                    Logger.logger.log("daze takoe bivaet")
                } else if (buffer[0] == 0xFF.toByte() && packet.length == 3) {
                    val id = ((buffer[1].toInt() and 0xff) shl 8) or (buffer[2].toInt() and 0xFF)
                    sendData.remove(id)
                } else {
                    val size =
                        ((buffer[10].toInt() and 0xFF) shl 8) or (buffer[11].toInt() and 0xFF)
                    val id = ((buffer[4].toInt() and 0xff) shl 8) or (buffer[5].toInt() and 0xff)
                    mapOfBytes.getOrPut(id, {
                        flowOfIds.emit(id)
                        Channel(Channel.UNLIMITED)
                    }).send(buffer.copyOfRange(0, size))
                }
            }
        }
        launch {
            flowOfIds.collect { id ->
                launch {
                    val channel = mapOfBytes[id]!!
                    var buffer = ByteArray(0)
                    buffer = channel.receive()
                    val totalCount = ((buffer[6].toInt() and 0xFF) shl 24) or
                            ((buffer[7].toInt() and 0xFF) shl 16) or
                            ((buffer[8].toInt() and 0xFF) shl 8) or
                            (buffer[9].toInt() and 0xFF)
                    if (totalCount == 1) {
                        messageReceiver.send(buffer)
                        mapOfBytes.remove(id)
                        return@launch
                    } else {
                        val receivedData = Array(totalCount) { false }
                        val massiveBuffer = ByteArray(totalCount * maxBytesInPacket)
                        val size =
                            ((buffer[10].toInt() and 0xFF) shl 8) or (buffer[11].toInt() and 0xFF)
                        val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                                ((buffer[1].toInt() and 0xFF) shl 16) or
                                ((buffer[2].toInt() and 0xFF) shl 8) or
                                (buffer[3].toInt() and 0xFF)
                        System.arraycopy(buffer, 0, massiveBuffer, 0, 20)
                        System.arraycopy(
                            buffer,
                            20,
                            massiveBuffer,
                            20 + (maxBytesInPacket - 20) * num,
                            size - 20
                        )
                        receivedData[num] = true
                        var totalSize = size
                        var count = 1
                        while (count < totalCount) {
                            try {
                                withTimeout(200) {
                                    buffer = channel.receive()
                                    val size =
                                        ((buffer[10].toInt() and 0xFF) shl 8) or (buffer[11].toInt() and 0xFF)
                                    val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                                            ((buffer[1].toInt() and 0xFF) shl 16) or
                                            ((buffer[2].toInt() and 0xFF) shl 8) or
                                            (buffer[3].toInt() and 0xFF)
                                    if (receivedData[num] != true) {
                                        receivedData[num] = true
                                        System.arraycopy(
                                            buffer,
                                            20,
                                            massiveBuffer,
                                            20 + (maxBytesInPacket - 20) * num,
                                            size - 20
                                        )
                                        count++
                                        totalSize += (size - 20)
                                    }
                                }
                            } catch (_: TimeoutCancellationException) {
                                receivedData.indices.filter { receivedData[it] == false }.forEach {
                                    socket.send(
                                        DatagramPacket(
                                            byteArrayOf(
                                                (id shr 8).toByte(),
                                                id.toByte(),
                                                (it shr 24).toByte(),
                                                (it shr 16).toByte(),
                                                (it shr 8).toByte(),
                                                it.toByte()
                                            ), 6, InetAddress.getByName(remoteIp), remotePort
                                        )
                                    )
                                }
                                continue
                            }
                        }
                        messageReceiver.send(massiveBuffer.copyOfRange(0, totalSize))
                    }
                }
            }
        }
    }

    fun closeChannel() {
        socket.close()
    }

}