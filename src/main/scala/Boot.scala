import actors.{AmqpListenerActor, AmqpPublisherActor}
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.Materializer
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import kz.mounty.fm.amqp.{AmqpConsumer, RabbitMQConnection}
import kz.mounty.fm.domain.room.{Room, RoomStatus}
import kz.mounty.fm.domain.user.{RoomUser, RoomUserType, UserProfile}
import kz.mounty.fm.serializers.{JodaCodec, Serializers}
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import scredis.Redis
import services.{PlayerService, RoomService, RoomUserService}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Boot extends App with Serializers {
  implicit val config: Config = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("mounty-room-core")
  implicit val mat: Materializer = Materializer(system)
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)

  val codecRegistry = fromRegistries(
    fromProviders(classOf[Room], classOf[RoomStatus], classOf[RoomUser], classOf[RoomUserType]),
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
    "Q:mounty-room-core-request-queue",
    "X:mounty-api-in",
    "mounty-messages.room-core.#"
  )

  RabbitMQConnection.declareExchange(
    channel,
    "X:mounty-spotify-gateway-out",
    "topic"
  ) match {
    case Success(value) => system.log.info("succesfully declared exchange")
    case Failure(exception) => system.log.warning(s"couldn't declare exchange ${exception.getMessage}")
  }

  RabbitMQConnection.declareAndBindQueue(
    channel,
    "Q:mounty-room-core-response-queue",
    "X:mounty-spotify-gateway-out",
    "mounty-messages.room-core.#"
  )

  implicit val publisher: ActorRef = system.actorOf(AmqpPublisherActor.props(channel))
  implicit val playerService: PlayerService = new PlayerService()
  implicit val roomService: RoomService = new RoomService()
  implicit val roomUserService: RoomUserService = new RoomUserService()
  val listener: ActorRef = system.actorOf(AmqpListenerActor.props())
  channel.basicConsume("Q:mounty-room-core-request-queue", AmqpConsumer(listener))
  channel.basicConsume("Q:mounty-room-core-response-queue", AmqpConsumer(listener))

}
