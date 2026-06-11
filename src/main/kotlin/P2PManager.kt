import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.DatagramSocket
import java.net.NetworkInterface
import java.util.logging.Logger

interface P2PLogger {
    fun d(message: String)
    fun e(message: String, error: Throwable? = null)
}

object P2PLog {
    var logger: P2PLogger? = null
}

class P2PManager {
    private val socket = DatagramSocket()
    private val address = StunManager.getAddress(socket)
    lateinit var channel: P2PChannel

    @Volatile
    var status = ""

    suspend fun getAddress() = withContext(Dispatchers.IO) { return@withContext address }
    suspend fun getLocalAddress() = withContext(Dispatchers.IO) {
        val int = NetworkInterface.getNetworkInterfaces().toList()
        var localIp = ""
        int.forEach { netInterface ->
            netInterface.inetAddresses.toList().forEach { address ->
                val addr = address.toString().replace("/", "").split(".")
                if (addr[0] == "192" && addr[addr.size - 1] != "1") {
                    localIp = address.toString().replace("/", "")
                }
            }
        }
        return@withContext "$localIp:${socket.localPort}"
    }

    suspend fun createConnection(remoteAddress: String, localAddress: String): P2PChannel {
        socket.soTimeout = 0
        channel = P2PChannel(socket)
        if (address?.split(":")[0] == remoteAddress.split(":")[0]) {
            channel.ip = localAddress.split(":")[0]
            channel.port = localAddress.split(":")[1].toInt()
        } else {
            channel.ip = remoteAddress.split(":")[0]
            channel.port = remoteAddress.split(":")[1].toInt()
        }
        val pingMessage = UDPFormat("ping", "ping")
        val connectMessage = UDPFormat("connect", "connect")
        GlobalScope.launch {
            launch {
                while (true) {
                    P2PLog.logger?.d(status)
                    try {
                        if (status == "ping") {
                            channel.send(connectMessage)
                        } else if (status == "connect") {
                            break
                        } else {
                            channel.send(pingMessage)
                        }
                        delay(500)
                    } catch (e: Exception) {
                        P2PLog.logger?.d(e.message.toString())
                    }
                }
            }
            launch {

                while (status != "connect") {
                    try {
                        P2PLog.logger?.d(status)
                        status = channel.receive(null).type
                    } catch (e: Exception) {
                        P2PLog.logger?.d(e.message.toString())
                    }
                }
            }
        }

        while (true) {
            if (status == "connect") {
                keepConnection()
                return channel
            }
            delay(100)
        }
    }

    fun keepConnection() = GlobalScope.launch {
        while (true) {
            channel.send(UDPFormat("keep", "keep"))
            delay(10000)
        }
    }
}