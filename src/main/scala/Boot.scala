import actors.{AmqpListenerActor, AmqpPublisherActor}
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.Materializer
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kz.mounty.fm.amqp.{AmqpConsumer, RabbitMQConnection}
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.{RoomUser, UserProfile}
import kz.mounty.fm.serializers.{JodaCodec, Serializers}
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.json4s.Serialization
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import scredis.Redis
import services.{PlayerService, RoomService}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Boot extends App with Serializers{
  implicit val config: Config = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("mounty-room-core")
  implicit val mat: Materializer = Materializer(system)
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)

  val codecRegistry = fromRegistries(
    fromProviders(classOf[Room]),
    CodecRegistries.fromCodecs(new JodaCodec()),
    DEFAULT_CODEC_REGISTRY)

  val client: MongoClient = MongoClient(config.getString("mongo.url"))
  val db: MongoDatabase = client
    .getDatabase(config.getString("mongo.db"))
    .withCodecRegistry(codecRegistry)

  implicit val roomUserCollection: MongoCollection[RoomUser] = db
    .getCollection[RoomUser](config.getString("mongo.room-user-col"))
  implicit val userProfileCollection: MongoCollection[UserProfile] = db
    .getCollection[UserProfile](config.getString("mongo.user-profile-col"))
  implicit val roomCollection: MongoCollection[Room] = db
    .getCollection[Room](config.getString("mongo.room-col"))

  val rmqHost = config.getString("rabbitmq.host")
  val rmqPort = config.getInt("rabbitmq.port")
  val username = config.getString("rabbitmq.username")
  val password = config.getString("rabbitmq.password")
  val virtualHost = config.getString("rabbitmq.virtualHost")

  implicit val redis: Redis = Redis(config.getConfig("redis"))

  val connection = RabbitMQConnection.rabbitMQConnection(
    username,
    password,
    rmqHost,
    rmqPort,
    virtualHost
  )

  val channel = connection.createChannel()

  RabbitMQConnection.declareExchange(
    channel,
    "X:mounty-api-in",
    "topic"
  ) match {
    case Success(value) => system.log.info("succesfully declared exchange")
    case Failure(exception) => system.log.warning(s"couldn't declare exchange ${exception.getMessage}")
  }

  RabbitMQConnection.declareAndBindQueue(
    channel,
    "Q:mounty-room-core-queue",
    "X:mounty-api-in",
    "mounty-messages.room-core.#"
  )

  RabbitMQConnection.declareExchange(
    channel,
    "X:spotify-gateway-in",
    "topic"
  ) match {
    case Success(value) => system.log.info("succesfully declared exchange")
    case Failure(exception) => system.log.warning(s"couldn't declare exchange ${exception.getMessage}")
  }

  RabbitMQConnection.declareAndBindQueue(
    channel,
    "Q:mounty-spotify-gateway-queue",
    "X:spotify-gateway-in",
    "mounty-messages.user-profile-core.#"
  )

  implicit val publisher: ActorRef = system.actorOf(AmqpPublisherActor.props(channel))
  implicit val playerService: PlayerService = new PlayerService()
  implicit val roomService: RoomService = new RoomService()
  val listener: ActorRef = system.actorOf(AmqpListenerActor.props())
  channel.basicConsume("Q:mounty-room-core-queue", AmqpConsumer(listener))
  channel.basicConsume("Q:mounty-spotify-gateway-queue", AmqpConsumer(listener))

}
