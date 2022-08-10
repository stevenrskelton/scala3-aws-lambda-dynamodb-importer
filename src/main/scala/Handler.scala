import com.amazonaws.lambda.thirdparty.com.fasterxml.jackson.annotation.JsonProperty
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.protocols.jsoncore.{JsonNode, JsonNodeParser}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import java.nio.file.Path
import java.util.Base64
import scala.jdk.CollectionConverters.*
import scala.util.Try

class StockPriceItem {
  @JsonProperty("stockId") var stockId: String = ""
  @JsonProperty("tradingDay") var tradingDay: String = ""
  @JsonProperty("proto") var proto: String = ""

  def toDynamoAttributeMap: java.util.Map[String, AttributeValue] = {
    if (stockId == null) throw new Exception("Missing `stockId`")
    if (tradingDay == null) throw new Exception("Missing `tradingDay`")
    if (proto == null) throw new Exception("Missing `proto`")

    val protoByteArray = Try(Base64.getDecoder.decode(proto)).getOrElse {
      throw new Exception(s"Could not decode base64: `$proto`")
    }
    Map(
      "stockId" -> AttributeValue.builder.n(stockId).build,
      "tradingDay" -> AttributeValue.builder.n(tradingDay).build,
      "proto" -> AttributeValue.builder.b(SdkBytes.fromByteArray(protoByteArray)).build
    ).asJava
  }
}

class Handler extends RequestHandler[java.util.List[StockPriceItem], String] {

  private val dynamoDbClient = {
    val path = Path.of("./src/main/resources/aws_credentials.txt")
    val credentialsProvider: AwsCredentialsProvider = if (path.toFile.exists) {
      val profileFile = ProfileFile.builder.content(path).`type`(ProfileFile.Type.CREDENTIALS).build
      ProfileCredentialsProvider.builder.profileFile(profileFile).build
    } else {
      DefaultCredentialsProvider.create()
    }
    DynamoDbClient.builder
      .credentialsProvider(credentialsProvider)
      .region(Region.US_EAST_1)
      .build
  }

  override def handleRequest(event: java.util.List[StockPriceItem], context: Context): String = {

    val lambdaLogger = context.getLogger
    lambdaLogger.log("event=" + event.asScala.map(o => s"stockId:${Option(o.stockId).getOrElse("[null]")},tradingDay:${Option(o.tradingDay).getOrElse("[null]")},proto:${Option(o.proto).getOrElse("[null]")}").mkString(","))

    val stockPrices = event.asScala
    stockPrices.foreach {
      stockPriceItem =>
        val request = PutItemRequest.builder.tableName("stockdaily_price").item(stockPriceItem.toDynamoAttributeMap).build
        val putItemResponse = dynamoDbClient.putItem(request)
        val sdkResponse = putItemResponse.sdkHttpResponse
        if (sdkResponse.isSuccessful) {
          lambdaLogger.log("Success")
        } else {
          val message = s"${sdkResponse.statusCode}${sdkResponse.statusText.map(" " + _)}"
          lambdaLogger.log(s"Error: $message")
          throw new Exception(message)
        }
    }

    "Success"
  }

}