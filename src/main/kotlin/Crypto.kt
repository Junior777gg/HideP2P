import com.google.crypto.tink.HybridDecrypt
import com.google.crypto.tink.HybridEncrypt
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.hybrid.HpkeParameters
import com.google.crypto.tink.hybrid.HybridConfig

class LocalCrypto {
    private val handle = KeysetHandle.generateNew(
        HpkeParameters.builder()
            .setKemId(HpkeParameters.KemId.DHKEM_X25519_HKDF_SHA256)
            .setKdfId(HpkeParameters.KdfId.HKDF_SHA256)
            .setAeadId(HpkeParameters.AeadId.AES_256_GCM)
            .setVariant(HpkeParameters.Variant.NO_PREFIX)
            .build()
    )

    private val decryptor: HybridDecrypt =
        handle.getPrimitive(RegistryConfiguration.get(), HybridDecrypt::class.java)

    val publicBytes: ByteArray =
        TinkProtoKeysetFormat.serializeKeysetWithoutSecret(handle.publicKeysetHandle)


    fun decrypt(data: ByteArray): ByteArray = decryptor.decrypt(data, publicBytes)

    companion object {
        init { HybridConfig.register() }
    }
}

class PeerCrypto(private val peerPublic: ByteArray) {
    private val encryptor: HybridEncrypt =
        TinkProtoKeysetFormat.parseKeysetWithoutSecret(peerPublic)
            .getPrimitive(RegistryConfiguration.get(), HybridEncrypt::class.java)

    fun encrypt(plain: ByteArray): ByteArray = encryptor.encrypt(plain, peerPublic)
}