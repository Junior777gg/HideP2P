import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.net.DatagramSocket

class P2PManager {
    private val socket = DatagramSocket()
    private val address = StunManager.getAddress(socket)
    lateinit var channel: P2PChannel

    @Volatile
    var status = ""

    fun getAddress() = address

    suspend fun createConnection(remoteAddress: String): P2PChannel {
        channel = P2PChannel(socket)
        channel.ip = remoteAddress.split(":")[0]
        channel.port = remoteAddress.split(":")[1].toInt()
        val pingMessage = UDPFormat("ping", "ping")
        val connectMessage = UDPFormat("connect", "connect")
        GlobalScope.launch {
            launch {
                while (true) {
                    if (status == "ping") {
                        channel.send(connectMessage)
                    } else if (status == "connect") {
                        break
                    } else {
                        channel.send(pingMessage)
                    }
                    delay(2000)
                }
            }
            launch {
                val currentStatus = channel.receive(null).type
                status = currentStatus
                if (currentStatus == "ping") {
                    channel.receive(null)
                    while (status != "connect") {
                        status = channel.receive(null).message
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
        
    }
}