import kotlinx.serialization.Serializable

@Serializable
data class UDPFormat(
    val type : String,
    val message : String,
)