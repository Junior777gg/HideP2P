import P2PManager.Companion.forkId
import P2PManager.Companion.forkInfo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.io.File
import java.io.InputStream
import java.io.RandomAccessFile
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import kotlin.math.ceil
import kotlin.random.Random

class P2PChannel(
    val tempDir: String,
    val socket: DatagramSocket,
    val inetSocketAddress: InetSocketAddress,
) {
    init {
        CoroutineScope(Dispatchers.IO).launch {
            internalReceive()
        }
    }

    internal var status = ""
    private val sendData = mutableMapOf<Int, MutableMap<Int, ByteArray>>()
    private val maxBytesInPacket = 1020
    private val messageReceiver = Channel<Messages>(Channel.UNLIMITED)

    suspend fun sendFork(bytes: ByteArray) = withContext(Dispatchers.IO) {
        val buffer = ByteArray(bytes.size + 8)
        System.arraycopy(bytes, 0, buffer, 8, bytes.size)
        socket.send(DatagramPacket(buffer, buffer.size, inetSocketAddress))
    }

    suspend fun send(rawData: ByteArray, code: Byte = 0.toByte()) = withContext(Dispatchers.IO) {
        val dataSize = rawData.size

        //Big packet (1020+ bytes)
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
                    buffer[18] = 0.toByte() // bytearray
                    buffer[19] = code
                    System.arraycopy(rawData, (maxBytesInPacket - 20) * i, buffer, 20, size - 20)
                    sendData.getOrPut(random) { mutableMapOf() }[i] = buffer
                    socket.send(DatagramPacket(buffer, size, inetSocketAddress))
                    Logger.logger.log("Sent message #$i (size: $size)")
                }
            } catch (e: Exception) {
                Logger.logger.log("Failed to send ByteArray: "+(e.message ?: "empty error"))
            }
        }
        //System packet
        else if (dataSize == 0) {
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
                buffer[18] = 0.toByte() // bytearray
                buffer[19] = code
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, 20, inetSocketAddress))
            } catch (e: Exception) {
                Logger.logger.log("Failed to send ByteArray: "+(e.message ?: "empty error"))
            }
        }
        //Normal packet
        else {
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
                buffer[18] = 0.toByte() // bytearray
                buffer[19] = code
                System.arraycopy(rawData, 0, buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, inetSocketAddress))
                Logger.logger.log("Sent message (size: ${buffer.size})")
            } catch (e: Exception) {
                Logger.logger.log("Failed to send ByteArray: "+(e.message ?: "empty error"))
            }
        }
    }

    suspend fun send(stream: InputStream, code: Byte) = withContext(Dispatchers.IO) {
        val dataSize = stream.available()

        //Big stream (1020+ bytes)
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
                    buffer[18] = 1.toByte() // file
                    buffer[19] = code
                    stream.readNBytes(buffer, 20, size - 20)
                    sendData.getOrPut(random) { mutableMapOf() }[i] = buffer
                    socket.send(DatagramPacket(buffer, size, inetSocketAddress))
                    Logger.logger.log(i.toString())
                }
            } catch (e: Exception) {
                Logger.logger.log("Failed to send InputStream: "+(e.message ?: "empty error"))
            } finally {
                stream.close()
            }
        }
        //Noraml/zero stream
        else {
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
                buffer[18] = 1.toByte() // file
                buffer[19] = code
                stream.readNBytes(buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, inetSocketAddress))
            } catch (e: Exception) {
                Logger.logger.log("Failed to send InputStream: "+(e.message ?: "empty error"))
            } finally {
                stream.close()
            }
        }
    }

    suspend fun send(file: File, code: Byte) = withContext(Dispatchers.IO) {
        val dataSize = file.length().toInt()
        val stream = file.inputStream()

        //Big file (1020+ bytes)
        if (dataSize + 20 > maxBytesInPacket) {
            try {
                val packetCount = ceil(dataSize.toFloat() / (maxBytesInPacket.toFloat() - 20f)).toInt()
                Logger.logger.log(packetCount.toString())
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
                    buffer[18] = 1.toByte() // file
                    buffer[19] = code
                    stream.readNBytes(buffer, 20, size - 20)
                    sendData.getOrPut(random) { mutableMapOf() }[i] = buffer
                    socket.send(DatagramPacket(buffer, size, inetSocketAddress))
                    Logger.logger.log(i.toString())
                }
            } catch (e: Exception) {
                Logger.logger.log("Failed to send File: "+(e.message ?: "empty error"))
            } finally {
                stream.close()
            }
        }
        //Normal/zero file
        else {
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
                buffer[18] = 1.toByte() // file
                buffer[19] = code
                stream.readNBytes(buffer, 20, dataSize)
                socket.send(DatagramPacket(buffer, buffer.size, inetSocketAddress))
            } catch (e: Exception) {
                Logger.logger.log("Failed to send File: "+(e.message ?: "empty error"))
            } finally {
                stream.close()
            }
        }
    }

    suspend fun receive(): Messages {
        while (true) {
            val message = messageReceiver.receive()
                try {
                    Logger.logger.log("Received message")
                    return message
                } catch (e: Exception) {
                    Logger.logger.log(e.message ?: "on receive error")
                    continue
                }
        }
    }

    internal suspend fun internalReceive(): Unit = withContext(Dispatchers.IO) {
        val mapOfBytes = mutableMapOf<Int, Channel<ByteArray>>()
        val flowOfIds = MutableSharedFlow<Int>(100)
        launch {
            while (true) {
                val buffer = ByteArray(maxBytesInPacket)
                val packet = DatagramPacket(buffer, buffer.size)
                socket.receive(packet)
                status = "new_packet"
                if (packet.length == 20) {
                    continue
                }
                if(buffer.copyOfRange(0,7).all {it == 0.toByte()}) {
                    forkInfo[forkId] = buffer.copyOfRange(8, packet.length).decodeToString()
                    continue
                }
                if (packet.length == 6) {
                    val currentId = ((buffer[0].toInt() and 0xFF) shl 8) or (buffer[1].toInt() and 0xFF)
                    val currentNum = ((buffer[2].toInt() and 0xFF) shl 24) or
                            ((buffer[3].toInt() and 0xFF) shl 16) or
                            ((buffer[4].toInt() and 0xFF) shl 8) or
                            (buffer[5].toInt() and 0xFF)
                    val data = sendData[currentId]!![currentNum]
                    socket.send(
                        DatagramPacket(data, data!!.size, inetSocketAddress)
                    )
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
                    val isArray = if (buffer[18] == 0.toByte()) true else false
                    val code = buffer[19]
                    if (totalCount == 1) {
                        if (isArray) {
                            messageReceiver.send(
                                Messages.ByteMessage(buffer.copyOfRange(20, buffer.size), code)
                            )
                            mapOfBytes.remove(id)
                            return@launch
                        } else {
                            val file = File(
                                tempDir,
                                Random.nextLong().toString()
                            ).apply {
                                createNewFile()
                                writeBytes(buffer.copyOfRange(20, buffer.size))
                            }
                            messageReceiver.send(
                                Messages.FileMessage(file, code)
                            )
                            mapOfBytes.remove(id)
                            return@launch
                        }
                    } else {
                        val receivedData = Array(totalCount) { false }
                        val raf = File(tempDir, Random.nextLong().toString()).apply {
                            createNewFile()
                        }
                        val massiveBuffer = RandomAccessFile(raf, "rw")

                        val size =
                            ((buffer[10].toInt() and 0xFF) shl 8) or (buffer[11].toInt() and 0xFF)
                        val num = ((buffer[0].toInt() and 0xFF) shl 24) or
                                ((buffer[1].toInt() and 0xFF) shl 16) or
                                ((buffer[2].toInt() and 0xFF) shl 8) or
                                (buffer[3].toInt() and 0xFF)
                        massiveBuffer.seek(((maxBytesInPacket - 20) * num).toLong())
                        massiveBuffer.write(buffer, 20, size - 20)
                        receivedData[num] = true
                        var totalSize = size - 20
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
                                        massiveBuffer.seek(((maxBytesInPacket - 20) * num).toLong())
                                        massiveBuffer.write(buffer, 20, size - 20)
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
                                            ), 6, inetSocketAddress
                                        )
                                    )
                                }
                                continue
                            }
                        }
                        massiveBuffer.close()
                        if (isArray) {
                            messageReceiver.send(Messages.ByteMessage(raf.readBytes(), code))
                        } else {
                            messageReceiver.send(Messages.FileMessage(raf, code))
                        }
                    }
                }
            }
        }
    }

    fun closeChannel() {
        socket.close()
    }
}