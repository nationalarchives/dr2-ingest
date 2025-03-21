package uk.gov.nationalarchives.lib

import kotlinx.serialization.Contextual
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import java.net.URI
import java.util.*

object JsonUtils {

    object UUIDSerializer : KSerializer<UUID> {
        override val descriptor = PrimitiveSerialDescriptor("UUID", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    }

    object URISerializer : KSerializer<URI> {
        override val descriptor = PrimitiveSerialDescriptor("URI", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder): URI = URI.create(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: URI) = encoder.encodeString(value.toString())
    }

    @Serializable
    data class TDRMetadata(
        val Series: String?,
        @Contextual val UUID: UUID?,
        val description: String?,
        val TransferringBody: String,
        val TransferInitiatedDatetime: String?,
        val ConsignmentReference: String,
        val Filename: String,
        val SHA256ServerSideChecksum: String,
        val FileReference: String
    )

    val jsonCodec = Json {
        serializersModule = SerializersModule {
            contextual(UUID::class, UUIDSerializer)
            contextual(URI::class, URISerializer)
        }
        ignoreUnknownKeys = true
    }

    @Serializable
    data class SFNArguments(val groupId: String, val batchId: String, val waitFor: Int, val retryCount: Int)

    @Serializable
    data class AggregatorInputMessage(@Contextual val id: UUID, @Contextual val location: URI)

    @Serializable
    data class ValidationErrorMessage(val error: String, @Contextual val fileId: UUID)

    @Serializable
    data class SqsInputMessage(@Contextual val fileId: UUID, val bucket: String)

    @Serializable
    data class ExternalNotificationMessage(val body: ExternalNotificationBody)

    @Serializable
    data class ExternalNotificationBody(
        val properties: ExternalNotificationProperties,
        val parameters: ExternalNotificationParameters
    )

    @Serializable
    data class ExternalNotificationParameters(
        @Serializable(with = UUIDSerializer::class) val assetId: UUID,
        val status: String
    )

    @Serializable
    data class ExternalNotificationProperties(val executionId: String, val messageType: String)

}