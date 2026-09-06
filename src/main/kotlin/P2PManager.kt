import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress

data class ICECandidates(
    val ipv4: InetSocketAddress?,
    val localIpv4: InetSocketAddress?,
    val ipv6: InetSocketAddress?,
    val localIpv6: InetSocketAddress?,
)

interface Log {
    fun log(message: String)
}

object Logger {
    lateinit var logger: Log
}

class P2PManager(val tempDir: String) {
    companion object {
        val forkInfo = mutableMapOf<Int, String>()
        @Volatile var forkId = 0
        fun getICEFromString(iceCandidates: String): ICECandidates {
            val split = iceCandidates.split("&")
            return ICECandidates(
                if (split[0] == "null" || split[1] == "null") {
                    null
                } else {
                    InetSocketAddress(InetAddress.getByName(split[0]), split[1].toInt())
                },
                if (split[2] == "null" || split[3] == "null") {
                    null
                } else {
                    InetSocketAddress(InetAddress.getByName(split[2]), split[3].toInt())
                },
                if (split[4] == "null" || split[5] == "null") {
                    null
                } else {
                    InetSocketAddress(InetAddress.getByName(split[4]), split[5].toInt())
                },
                if (split[6] == "null" || split[7] == "null") {
                    null
                } else {
                    InetSocketAddress(InetAddress.getByName(split[6]), split[7].toInt())
                },

                )
        }
    }

    suspend fun fork(): P2PManager? {
        return try {
            val manager = P2PManager(tempDir)
            channel.sendFork(manager.getICECandidates().encodeToByteArray())
            while (forkInfo[forkId] == null) {
                delay(100)
            }
            manager.createConnection(getICEFromString(forkInfo[forkId]!!))
            forkId++
            Logger.logger.log("fork successful")
            manager
        } catch (e: Exception) {
            Logger.logger.log(e.message ?: "fork failed")
            null
        }
    }

    private lateinit var keepScope: Job
    private val myIpv4LocalAddress = StunManager.getLocalAddress(false)
    private val myIpv6LocalAddress = StunManager.getLocalAddress(true)
    private val socketIpv4 = try {
        DatagramSocket(myIpv4LocalAddress)
    } catch (_: Exception) {
        null
    }
    private val socketIpv6 = try {
        DatagramSocket(myIpv6LocalAddress)
    } catch (_: Exception) {
        null
    }
    private val myIpv4Address: InetSocketAddress? by lazy {
        if (socketIpv4 != null) StunManager.getAddress(socketIpv4, false) else null
    }
    private val myIpv6Address: InetSocketAddress? by lazy {
        if (socketIpv6 != null) StunManager.getAddress(socketIpv6, true) else null
    }

    lateinit var channel: P2PChannel
    val pingMessage = ByteArray(0)

    @Volatile
    var status = ""

    fun getICECandidates(): String {
        return buildString {
            append(myIpv4Address?.hostString ?: "null", "&")
            append(myIpv4Address?.port ?: "null", "&")
            append(myIpv4LocalAddress?.hostString ?: "null", "&")
            append(myIpv4LocalAddress?.port ?: "null", "&")
            append(myIpv6Address?.hostString ?: "null", "&")
            append(myIpv6Address?.port ?: "null", "&")
            append(myIpv6LocalAddress?.hostString ?: "null", "&")
            append(myIpv6LocalAddress?.port ?: "null")
        }
    }

    suspend fun createConnection(iceCandidates: ICECandidates): P2PChannel {
        socketIpv4?.soTimeout = 0
        socketIpv6?.soTimeout = 0
        CoroutineScope(Dispatchers.IO + SupervisorJob()).launch {
            try {
                when {
                    iceCandidates.ipv6 != null && myIpv6Address != null -> {
                        if (iceCandidates.ipv6.hostString != myIpv6Address!!.hostString) {
                            channel = P2PChannel(tempDir, socketIpv6!!, iceCandidates.ipv6)
                        } else {
                            channel = P2PChannel(tempDir, socketIpv6!!, iceCandidates.localIpv6!!)
                        }
                    }

                    iceCandidates.ipv4 != null && myIpv4Address != null -> {
                        if (iceCandidates.ipv4.hostString != myIpv4Address!!.hostString) {
                            channel = P2PChannel(tempDir, socketIpv4!!, iceCandidates.ipv4)
                        } else {
                            channel = P2PChannel(tempDir, socketIpv4!!, iceCandidates.localIpv4!!)
                        }
                    }
                }
            } catch (e: Exception) {
                Logger.logger.log(e.message ?: "Connection failed")
            }
            launch {
                while (true) {
                    try {
                        if (status == "") {
                            channel.send(pingMessage)
                        } else {
                            for (i in 0 until 5) {
                                channel.send(pingMessage)
                                delay(100)
                            }
                            break
                        }
                        delay(1000)
                    } catch (e: Exception) {
                        Logger.logger.log(e.message ?: "Connection failed")
                    }
                }
            }
            launch {
                while (status == "") {
                    status = channel.status
                    delay(500)
                }
                Logger.logger.log(status)
            }
        }

        while (true) {
            if (status != "") {
                keepConnection()
                return channel
            }
            delay(100)
        }
    }

    internal fun keepConnection() {
        keepScope = CoroutineScope(Dispatchers.IO).launch {
            while (true) {
                if (this.isActive) {
                    channel.send(pingMessage)
                    delay(5000)
                }
            }
        }
    }

    fun breakConnection() {
        keepScope.cancel()
        channel.closeChannel()
    }
}