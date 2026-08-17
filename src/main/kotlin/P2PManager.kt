import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.net.DatagramSocket
import java.net.NetworkInterface

interface Log {
    fun log(message: String)
}

object Logger {
    lateinit var logger: Log
}

class P2PManager(val tempDir: String) {
    companion object {
        var forkInfo = ""
    }

    suspend fun fork(): P2PManager?{
        return try {
            val manager = P2PManager(tempDir)
            channel.sendFork("${manager.getAddress()}&${manager.getLocalAddress()}".encodeToByteArray())
            while (forkInfo == ""){
                delay(100)
            }
            val split = forkInfo.split("&")
            manager.createConnection(split[0], split[1])
            Logger.logger.log("fork successful")
            manager
        }catch (_: Exception){
            null
        }
    }

    private lateinit var keepScope: Job
    private val socket = DatagramSocket()
    private val address = StunManager.getAddress(socket)
    lateinit var channel: P2PChannel
    val pingMessage = ByteArray(0)
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

    suspend fun createConnection(
        remoteAddress: String,
        remoteLocalAddress: String,
    ): P2PChannel {
        socket.soTimeout = 0
        channel = P2PChannel(tempDir,socket)
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
        GlobalScope.launch {
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
                        Logger.logger.log(e.message ?: "")
                    }
                }
            }
            launch {
                while (status == "") {
                    status = channel.status
                    Logger.logger.log(status)
                    delay(500)               }
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