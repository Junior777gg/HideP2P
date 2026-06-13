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

    suspend fun createConnection(remoteAddress: String, remoteLocalAddress: String): P2PChannel {
        socket.soTimeout = 0
        channel = P2PChannel(socket)
        if (address?.split(":")[0] == remoteAddress.split(":")[0]) {
            val myLocalAddress = getLocalAddress()
            channel.myIp = myLocalAddress.split(":")[0]
            channel.myPort = myLocalAddress.split(":")[1].toInt()
            channel.remoteIp = remoteLocalAddress.split(":")[0]
            channel.remotePort = remoteLocalAddress.split(":")[1].toInt()
        } else {
            val myAddress = getAddress()
            channel.myIp = myAddress!!.split(":")[0]
            channel.myPort = myAddress.split(":")[1].toInt()
            channel.remoteIp = remoteAddress.split(":")[0]
            channel.remotePort = remoteAddress.split(":")[1].toInt()
        }
        val pingMessage = ByteArray(0)
        GlobalScope.launch {
            launch {
                while (true) {
                    try {
                        if (status == ""){
                            channel.send(pingMessage)
                        }else{
                            for(i in 0 until 5){
                                channel.send(pingMessage)
                                delay(50)
                            }
                            break
                        }
                        delay(500)
                    } catch (e: Exception) {
                        P2PLog.logger?.d(e.message.toString())
                    }
                }
            }
            launch {
                while (status == "") {
                    try {
                        P2PLog.logger?.d(status)
                        channel.internalReceive().let {
                            status = "new_packet"
                        }
                    } catch (e: Exception) {
                        P2PLog.logger?.d(e.message.toString())
                    }
                }
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

    fun keepConnection() = GlobalScope.launch {
        while (true) {
            channel.send(ByteArray(0))
            delay(10000)
        }
    }
}