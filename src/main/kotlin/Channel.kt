import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.io.ByteArrayInputStream
import java.io.File
import java.io.RandomAccessFile
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.BitSet
import java.util.concurrent.ConcurrentHashMap
import kotlin.Any
import kotlin.collections.List
import kotlin.collections.mutableMapOf
import kotlin.math.ceil
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

data class Packet(
    val id: Int,
    val random: Int,
    val dataCount: Int,
    val packetType: Int,
    val className: String,
    val number: Int,
    val dataCounter: Int,
    val packetCount: Int,
    val data: ByteArray
)

interface Message
data class Text(val text: String) : Message
data class Bytes(val bytes: ByteArray) : Message
data class Files(val file: File) : Message

class P2PChannel(
    val tempDir: String,
    private val socket: DatagramSocket,
    private val inetSocketAddress: InetSocketAddress,
) {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val internalReceiver = MutableSharedFlow<List<Any>>(1000)
    val receiver = MutableSharedFlow<Message>(1000)
    private val map = ConcurrentHashMap<Int, Channel<Packet>>()
    private val sendData = ConcurrentHashMap<Int, Message>()

    init {
        scope.launch {
            launch {
                receive()
            }
            launch {
                val savedMessages = ConcurrentHashMap<Int, MutableMap<Int, Message>>()
                internalReceiver.collect { message ->
                    val id = message[1] as Int
                    val size = message[0] as Int
                    val number = message[2] as Int
                    val messageClass = message[3] as Message
                    if (size == 1) {
                        receiver.emit(messageClass)
                    } else {
                        if (savedMessages[id]?.size == size - 1) {
                            savedMessages[id]!![number] = messageClass
                            for (i in 0 until size) {
                                receiver.emit(savedMessages[id]!![i]!!)
                            }
                            savedMessages.remove(id)
                        } else {
                            savedMessages.getOrPut(id) { mutableMapOf() }[number] = messageClass
                        }
                    }

                }
            }
        }

    }

    internal var status = ""

    private suspend fun sendFork(bytes: ByteArray) = withContext(Dispatchers.IO) {
        val buffer = ByteArray(bytes.size + 8)
        System.arraycopy(bytes, 0, buffer, 8, bytes.size)
        socket.send(DatagramPacket(buffer, buffer.size, inetSocketAddress))
    }

    suspend fun sendPing() = withContext(Dispatchers.IO) {
        socket.send(DatagramPacket(byteArrayOf(), 0, inetSocketAddress))
    }

    private suspend fun sendData(any: Any, counter: Int, id: Int, arraySize: Int) = withContext(Dispatchers.IO) {
        val random = Random.nextInt(65536)
        val buffer = ByteArray(1020)
        val stream = when (any) {
            is ByteArray -> {
                buffer[12] = 0
                sendData[random] = Bytes(any)
                ByteArrayInputStream(any)
            }

            is String -> {
                buffer[12] = 1
                sendData[random] = Text(any)
                ByteArrayInputStream(any.encodeToByteArray())
            }

            is File -> {
                buffer[12] = 2
                sendData[random] = Files(any)
                any.inputStream()
            }

            else -> return@withContext
        }
        val dataSize = stream.available()
        val packetCount = ceil(dataSize.toFloat() / (1000f)).toInt()
        for (i in 0 until packetCount) {
            val size = minOf(1000, (dataSize - 1000 * i))
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
            buffer[14] = counter.toByte()
            buffer[13] = 0 //data
            buffer[15] = (id shr 8).toByte()
            buffer[16] = id.toByte()
            buffer[17] = arraySize.toByte()
            stream.readNBytes(buffer, 20, size)
            socket.send(DatagramPacket(buffer.copyOfRange(0, size + 20), size + 20, inetSocketAddress))
        }
    }

    //добирать ли потерянные пакеты, интерфейс для переделывания кастомных классов в байт сарай
    suspend fun send(vararg data: Any): Int = withContext(Dispatchers.IO) {
        val random = Random.nextInt(65536)
        for ((counter, one) in data.withIndex()) {
            launch {
                sendData(one, counter, random, data.size)
            }
            delay(5)
        }
        return@withContext 0
    }

    private fun decodeHeader(buffer: ByteArray): Packet {
        val number = ((buffer[0].toInt() and 0xFF) shl 24) or
                ((buffer[1].toInt() and 0xFF) shl 16) or
                ((buffer[2].toInt() and 0xFF) shl 8) or
                (buffer[3].toInt() and 0xFF)
        val id = ((buffer[15].toInt() and 0xff) shl 8) or (buffer[16].toInt() and 0xff)
        val random = ((buffer[4].toInt() and 0xff) shl 8) or (buffer[5].toInt() and 0xff)
        val count = ((buffer[6].toInt() and 0xFF) shl 24) or
                ((buffer[7].toInt() and 0xFF) shl 16) or
                ((buffer[8].toInt() and 0xFF) shl 8) or
                (buffer[9].toInt() and 0xFF)
        val data = buffer.copyOfRange(20, buffer.size)
        val className = when (buffer[12].toInt() and 0xFF) {
            0 -> "ByteArray"
            1 -> "String"
            2 -> "File"
            else -> "Unknown"
        }
        val type = buffer[13].toInt() and 0xFF
        val counter = buffer[14].toInt() and 0xFF
        val dataCount = buffer[17].toInt() and 0xFF
        return Packet(id, random, dataCount, type, className, number, counter, count, data)
    }

    private suspend fun findLostPackets(
        random: Int,
        numbers: Array<Int>,
        packetCount: Int,
        dataCount: Int,
        id: Int,
        counter: Int
    ) = withContext(
        Dispatchers.IO
    ) {
        val size = numbers.size
        val buffer = ByteArray(1020)
        buffer[0] = (size shr 24).toByte()
        buffer[1] = (size shr 16).toByte()
        buffer[2] = (size shr 8).toByte()
        buffer[3] = size.toByte()
        buffer[4] = (random shr 8).toByte()
        buffer[5] = random.toByte()
        buffer[6] = (packetCount shr 24).toByte()
        buffer[7] = (packetCount shr 16).toByte()
        buffer[8] = (packetCount shr 8).toByte()
        buffer[9] = packetCount.toByte()
        buffer[10] = (size shr 8).toByte()
        buffer[11] = size.toByte()
        buffer[14] = counter.toByte()
        buffer[13] = 1 //find lost
        buffer[15] = (id shr 8).toByte()
        buffer[16] = id.toByte()
        buffer[17] = dataCount.toByte()
        for ((i, num) in numbers.withIndex()) {
            buffer[20 + i * 4] = (num shr 24).toByte()
            buffer[21 + i * 4] = (num shr 16).toByte()
            buffer[22 + i * 4] = (num shr 8).toByte()
            buffer[23 + i * 4] = num.toByte()
        }
        socket.send(DatagramPacket(buffer.copyOfRange(0, size * 4 + 20), size * 4 + 20, inetSocketAddress))
    }

    private fun getEmptyBits(bitSet: BitSet, limit: Int): Array<Int> {
        val emptyBits = mutableListOf<Int>()
        var int = bitSet.nextClearBit(0)
        while (int < limit) {
            emptyBits.add(int)
            int = bitSet.nextClearBit(int + 1)
        }
        return emptyBits.toTypedArray()
    }

    private suspend fun removeData(random: Int) = withContext(Dispatchers.IO) {
        val buffer = ByteArray(20)
        buffer[4] = (random shr 8).toByte()
        buffer[5] = random.toByte()
        buffer[13] = 2 //remove data
        socket.send(DatagramPacket(buffer, 20, inetSocketAddress))
    }

    private suspend fun merger(random: Int, count: Int, className: String) = withContext(Dispatchers.IO) {
        val channel = map[random]!!
        val file = File(tempDir, Random.nextLong().toString()).apply {
            createNewFile()
        }
        var dataCount = 0
        var id = 0
        var dataCounter = 0
        val bitset = BitSet(count)
        val raf = RandomAccessFile(file, "rw")
        while (bitset.cardinality() < count) {
            try {
                val packet = withTimeout(2.seconds) {
                    channel.receive()
                }
                dataCount = packet.dataCount
                id = packet.id
                dataCounter = packet.dataCounter
                val data = packet.data
                raf.seek(packet.number * 1000L)
                raf.write(data)
                bitset.set(packet.number)
            } catch (_: TimeoutCancellationException) {
                val emptyBits = getEmptyBits(bitset, count)
                findLostPackets(random, emptyBits, count, dataCount, id, dataCounter)
                continue
            }
        }
        raf.close()
        removeData(random)
        when (className) {
            "ByteArray" -> {
                val bytes = file.readBytes()
                internalReceiver.emit(listOf(dataCount, id, dataCounter, Bytes(bytes)))
                file.delete()
            }

            "String" -> {
                val bytes = file.readBytes()
                internalReceiver.emit(listOf(dataCount, id, dataCounter, Text(bytes.decodeToString())))
                file.delete()
            }

            "File" -> {
                internalReceiver.emit(listOf(dataCount, id, dataCounter, Files(file)))
            }
        }
        map.remove(random)
    }

    private suspend fun sendLostPackets(packet: Packet) = withContext(Dispatchers.IO) {
        val data = packet.data
        val nums: List<Int> = buildList {
            for (i in 0 until packet.number) {
                val num = ((data[i * 4].toInt() and 0xFF) shl 24) or
                        ((data[1 + i * 4].toInt() and 0xFF) shl 16) or
                        ((data[2 + i * 4].toInt() and 0xFF) shl 8) or
                        (data[3 + i * 4].toInt() and 0xFF)
                add(num)
            }
        }
        val message = sendData[packet.random]
        val random = packet.random
        val buffer = ByteArray(1020)
        val stream = when (message) {
            is Bytes -> {
                buffer[12] = 0
                ByteArrayInputStream(message.bytes)
            }

            is Text -> {
                buffer[12] = 1
                ByteArrayInputStream(message.text.toByteArray())
            }

            is Files -> {
                buffer[12] = 2
                message.file.inputStream()
            }

            else -> return@withContext
        }
        val dataSize = stream.available()
        val packetCount = ceil(dataSize.toFloat() / (1000f)).toInt()
        for (i in 0 until packetCount) {
            val size = minOf(1000, (dataSize - 1000 * i))
            if (!nums.contains(i)) {
                stream.skipNBytes(size.toLong())
                continue
            }
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
            buffer[14] = packet.dataCounter.toByte()
            buffer[13] = 0 //data
            buffer[15] = (packet.id shr 8).toByte()
            buffer[16] = packet.id.toByte()
            buffer[17] = packet.dataCount.toByte()
            stream.readNBytes(buffer, 20, size)
            socket.send(DatagramPacket(buffer.copyOfRange(0, size + 20), size + 20, inetSocketAddress))
        }
    }

    private suspend fun receive() = withContext(Dispatchers.IO) {
        while (true) {
            try {
                val buffer = ByteArray(1020)
                val datagramPacket = DatagramPacket(buffer, buffer.size)
                socket.receive(datagramPacket)
                status = "first"
                if (datagramPacket.length < 20) continue
                val packet = decodeHeader(buffer.copyOfRange(0, datagramPacket.length))
                if (packet.packetType == 1) {
                    sendLostPackets(packet)
                    continue
                }
                if (packet.packetType == 2) {
                    sendData.remove(packet.random)
                    continue
                }
                if (packet.packetCount == 1) {
                    launch {
                        val message = when (packet.className) {
                            "ByteArray" -> {
                                Bytes(packet.data)
                            }

                            "String" -> {
                                Text(packet.data.decodeToString())
                            }

                            "File" -> {
                                val file = File(tempDir, Random.nextLong().toString()).apply {
                                    createNewFile()
                                    writeBytes(packet.data)
                                }
                                Files(file)
                            }

                            else -> cancel()
                        }
                        internalReceiver.emit(
                            listOf(
                                packet.dataCount,
                                packet.id,
                                packet.dataCounter,
                                message
                            )
                        )
                        removeData(packet.random)
                        cancel()
                    }
                } else if (map[packet.random] == null) {
                    map.getOrPut(packet.random) { Channel(Channel.UNLIMITED) }.send(packet)
                    merger(packet.random, packet.packetCount, packet.className)
                    delay(50)
                } else if (map[packet.random] != null) {
                    map[packet.random]!!.send(packet)
                }

            } catch (e: Exception) {
                Logger.logger.log {
                    e.printStackTrace()
                }
                continue
            }
        }
    }

    fun closeChannel() {
        socket.close()
        scope.cancel()
    }
}