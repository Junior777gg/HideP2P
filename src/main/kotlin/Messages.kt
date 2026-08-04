import java.io.File

sealed class Messages {
    class ByteMessage(val bytes: ByteArray, val code: Byte) : Messages()
    class FileMessage(val file: File, val code: Byte) : Messages()
}