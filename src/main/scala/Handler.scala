import java.nio.file.Path
import java.util.Base64

import com.amazonaws.services.lambda.runtime.events.{APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse}
import com.amazonaws.services.lambda.runtime.{Context, LambdaLogger, RequestHandler}

import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider, ProfileCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.protocols.jsoncore.{JsonNode, JsonNodeParser}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*
import scala.util.{Failure, Success, Try}

val tableName = "demo_stock_prices"

val dynamoDbClient =
  val path = Path.of("./src/main/resources/aws_credentials.txt")
  val credentialsProvider: AwsCredentialsProvider = if (path.toFile.exists) {
    val profileFile = ProfileFile.builder.content(path).`type`(ProfileFile.Type.CREDENTIALS).build
    ProfileCredentialsProvider.builder.profileFile(profileFile).build
  } else {
    DefaultCredentialsProvider.create
  }
  DynamoDbClient.builder
    .credentialsProvider(credentialsProvider)
    .region(Region.US_EAST_1)
    .build

def errorToResult(ex: Throwable)(using lambdaLogger: LambdaLogger): APIGatewayV2HTTPResponse =
  ex match
    case ParseException(error, content) =>
      val message = s"Error parsing request $error in $content"
      lambdaLogger.log(message)
      APIGatewayV2HTTPResponse.builder.withStatusCode(400).withBody(message).build
    case _ =>
      throw ex

def parseStockPriceItems(json: String)(using lambdaLogger: LambdaLogger): Try[Iterable[StockPriceItem]] =
  lambdaLogger.log(json)
  Try {
    val items = JsonNodeParser.create.parse(json).asArray.asScala.map(StockPriceItem.apply)
    val msg = "event=" + items.map {
      item => s"symbol:${item.symbol},time:${item.time},prices:${item.prices}"
    }.mkString(",")
    lambdaLogger.log(msg)
    items
  }

def putIntoDynamoDB(stockPriceItems: Iterable[StockPriceItem])(using lambdaLogger: LambdaLogger): Try[Long] = Try {
  val addedTimes = stockPriceItems.map {
    stockPriceItem =>
      val request = PutItemRequest.builder.tableName(tableName).item(stockPriceItem.dynamoDBAttributeMap).build
      val putItemResponse = dynamoDbClient.putItem(request)
      val sdkResponse = putItemResponse.sdkHttpResponse
      if (sdkResponse.isSuccessful) {
        lambdaLogger.log("Success")
        stockPriceItem.time.toLong
      } else {
        val message = s"${sdkResponse.statusCode}${sdkResponse.statusText.map(" " + _)}"
        lambdaLogger.log(s"Error: $message")
        throw new Exception(message)
      }
  }
  addedTimes.max
}

case class ParseException(error: String, content: String) extends Exception(error)

class Handler extends RequestHandler[APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse] :

  override def handleRequest(event: APIGatewayV2HTTPEvent, context: Context): APIGatewayV2HTTPResponse =
    given lambdaLogger: LambdaLogger = context.getLogger

    lambdaLogger.log("Start")
    lambdaLogger.log(event.getBody)

    val body = Option(event.getBody).withFilter(!_.isBlank)
      .map(Success(_))
      .getOrElse(Failure(ParseException("empty body", "''")))

    body
      .flatMap(parseStockPriceItems)
      .flatMap(putIntoDynamoDB)
      .fold(errorToResult, maxTimeAdded =>
        APIGatewayV2HTTPResponse.builder
          .withStatusCode(200)
          .withBody(s"""{ "maxTimeAdded": $maxTimeAdded }""")
          .build
      )
