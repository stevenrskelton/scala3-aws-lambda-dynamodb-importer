import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.profiles.ProfileFile
import software.amazon.awssdk.protocols.jsoncore.JsonNodeParser
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}

import scala.jdk.CollectionConverters.*
import java.nio.file.Path
import java.util.Base64

class Handler extends RequestHandler[Map[String, String], String] {

  private val jsonNodeParser = JsonNodeParser.create

  private val dynamoDbClient = {
    val path = Path.of("./src/main/resources/aws_credentials.txt")
    val profileFile = ProfileFile.builder.content(path).`type`(ProfileFile.Type.CREDENTIALS).build
    val credentialsProvider = ProfileCredentialsProvider.builder.profileFile(profileFile).build
    DynamoDbClient.builder
      .credentialsProvider(credentialsProvider)
      .region(Region.US_EAST_1)
      .build
  }

  override def handleRequest(event: Map[String, String], context: Context): String = {

    val json = "{\"title\":\"Thinking in Java\",\"isbn\":\"978-0131872486\"" +
      ",\"year\":1998,\"authors\":[\"Bruce Eckel\"]}";
    
    val attributeNodes = jsonNodeParser.parse(event.get("body?").get).asArray.asScala
    val item = attributeNodes.map {
      jsNode =>
        val jsonObject = jsNode.asObject
        val attributeName = jsonObject.get("name").asString
        val attributeValue = if(jsonObject.containsKey("n")){
          val n = jsonObject.get("n").asString
          AttributeValue.builder.n(n).build
        }else if(jsonObject.containsKey("s")) {
          val s = jsonObject.get("s").asString
          AttributeValue.builder.s(s).build
        }else if(jsonObject.containsKey("b")) {
          val base64 = jsonObject.get("b").asString
          val b = SdkBytes.fromByteArray(Base64.getDecoder.decode(base64))
          AttributeValue.builder.b(b).build
        }else{
          val attributeKeys = jsonObject.keySet.asScala.withFilter(_ != "name").map("`" + _ + "`").mkString(",")
          throw new Exception(s"Unknown attribute type in $attributeKeys")
        }
        (attributeName, attributeValue)
    }.toMap.asJava

    val request = PutItemRequest.builder.tableName("stockdaily_price").item(item).build
    val putItemResponse = dynamoDbClient.putItem(request)
    val sdkResponse = putItemResponse.sdkHttpResponse
    if(sdkResponse.isSuccessful){
      putItemResponse.consumedCapacity.capacityUnits.toString
    }else{
      val message = s"${sdkResponse.statusCode}${sdkResponse.statusText.map(" " + _)}"
      throw new Exception(message)
    }
  }

}