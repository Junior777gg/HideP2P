import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import com.google.crypto.tink.JsonKeysetReader
import com.google.crypto.tink.JsonKeysetWriter
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.config.TinkConfig
import com.google.crypto.tink.hybrid.HybridKeyTemplates
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.net.DatagramSocket
import java.net.NetworkInterface

interface Log {
    fun log(message: String)
}

object Logger {
    lateinit var logger: Log
}

class P2PManager {
    init {
        try {
            TinkConfig.register()
        } catch (e: Exception) {
            Logger.logger.log(e.message?:"")
        }
    }

    private lateinit var keepScope: Job
    private val socket = DatagramSocket()
    private val address = StunManager.getAddress(socket)
    lateinit var channel: P2PChannel

    @Volatile
    var status = ""
    private val myKeysetHandle = KeysetHandle.generateNew(
        HybridKeyTemplates.ECIES_P256_HKDF_HMAC_SHA256_AES128_GCM
    )
    private val myDecryptor: HybridDecrypt = myKeysetHandle.getPrimitive(HybridDecrypt::class.java)

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

    suspend fun getPublicKeyJson(): String = withContext(Dispatchers.IO) {
        val stream = ByteArrayOutputStream()
        CleartextKeysetHandle.write(
            myKeysetHandle.publicKeysetHandle,
            JsonKeysetWriter.withOutputStream(stream)
        )
        return@withContext stream.toString("UTF-8")
    }

    suspend fun createConnection(
        remoteAddress: String,
        remoteLocalAddress: String,
        peerPublicKeyJson: String,
    ): P2PChannel {
        val peerHandle = CleartextKeysetHandle.read(
            JsonKeysetReader.withBytes(peerPublicKeyJson.toByteArray())
        )
        val peerEncryptor = peerHandle.getPrimitive(HybridEncrypt::class.java)
        socket.soTimeout = 0
        channel = P2PChannel(socket, peerEncryptor, myDecryptor)
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
                        Logger.logger.log(e.message?:"")
                    }
                }
            }
            launch {
                while (status == "") {
                    try {
                        channel.internalReceive().let {
                            status = "new_packet"
                        }
                    } catch (e: Exception) {
                        Logger.logger.log(e.message?:"")
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

    internal fun keepConnection(){
        keepScope = CoroutineScope(Dispatchers.IO).launch {
            while (true) {
                if (this.isActive) {
                    channel.send(ByteArray(0))
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