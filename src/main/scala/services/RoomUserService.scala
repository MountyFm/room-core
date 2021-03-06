package services

import akka.actor.{ActorLogging, ActorRef}
import akka.util.Timeout
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.MountyApi
import kz.mounty.fm.domain.requests._
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.{RoomUser, RoomUserType}
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ServerErrorRequestException}
import kz.mounty.fm.serializers.Serializers
import org.bson.conversions.Bson
import org.json4s.Formats
import org.mongodb.scala.MongoCollection
import repositories.RoomUserRepository
import scredis.Redis
import org.mongodb.scala.model.Filters.{and, equal}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import org.mongodb.scala.model.Updates.set

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class RoomUserService(implicit val redis: Redis,
                      publisher: ActorRef,
                      ex: ExecutionContext,
                      formats: Formats,
                      roomCol: MongoCollection[Room],
                      userRoomCol: MongoCollection[RoomUser],
                      timeout: Timeout) extends Serializers{
  val roomUserRepository = new RoomUserRepository

  def createRoomUserIfNotExist(profileId: String, roomId: String, `type`: RoomUserType): Future[RoomUser] = {
    roomUserRepository.findByFilter[RoomUser](and(equal("profileId", profileId), equal("roomId", roomId))).flatMap {
      case Some(roomUser) =>
        var updatedBson: Seq[Bson] = Seq()
        updatedBson :+= set("isActive", true)
        roomUserRepository.updateOneByFilter[RoomUser](equal("id", roomUser.id), updatedBson).onComplete {
          case Success(updated) =>
          case Failure(exception) =>
        }
        Future(roomUser.copy(isActive = true))
      case None =>
        val newRoomUser = RoomUser(
          id = UUID.randomUUID().toString,
          profileId = profileId,
          roomId = roomId,
          `type` = `type`,
          isActive = true
        )
        roomUserRepository.create[RoomUser](newRoomUser)
    }
  }

  def getRoomUsers(amqpMessage: AMQPMessage): Unit = {
    val request = parse(amqpMessage.entity).extract[GetRoomUsersRequestBody]

    val filter: Bson = request.`type` match {
      case Some(t) =>
        and(equal("roomId", request.roomId), equal("type", t))
      case None =>
        equal("roomId", request.roomId)
    }

    roomUserRepository.findAllByFilter[RoomUser](filter).map { roomUsers =>
      publisher ! amqpMessage.copy(
        entity = write(GetRoomUsersResponseBody(roomUsers)),
        routingKey = MountyApi.GetRoomUsersResponse.routingKey, exchange = "X:mounty-api-out")
    } recover {
      case exception: Throwable =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(exception.getMessage)
        ).getExceptionInfo
        publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def getRoomUserById(amqpMessage: AMQPMessage): Unit = {
    val roomUserId = parse(amqpMessage.entity).extract[GetRoomUserByIdRequestBody].id

    roomUserRepository.findByFilter[RoomUser](equal("profileId", roomUserId)).map {
      case Some(roomUser) =>
        val response = GetRoomUserByIdResponseBody(roomUser)
        publisher ! amqpMessage.copy(entity = write(response), routingKey = MountyApi.GetRoomUserByIdResponse.routingKey, exchange = "X:mounty-api-out")
      case None =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some("not found entity")
        ).getExceptionInfo
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    } recover {
      case e: Throwable =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(e.getMessage)
        ).getExceptionInfo
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def updateRoomUser(amqpMessage: AMQPMessage): Unit = {
    val parsedRequest = parse(amqpMessage.entity).extract[UpdateRoomUserRequestBody]

    var updatedBson: Seq[Bson] = Seq()
    if (parsedRequest.`type`.isDefined) updatedBson :+= set("type", parsedRequest.`type`.get)
    if (parsedRequest.isActive.isDefined) updatedBson :+= set("isActive", parsedRequest.isActive.get)

    if (updatedBson.nonEmpty) {
      roomUserRepository.updateOneByFilter[RoomUser](equal("id", parsedRequest.id), updatedBson).onComplete {
        case Success(value) =>
          val reply = write(UpdateRoomUserResponseBody(value))
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.UpdateRoomUserResponse.routingKey, exchange = "X:mounty-api-out")
        case Failure(exception) =>
          val error = ServerErrorRequestException(
            ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
            Some(exception.getMessage)
          ).getExceptionInfo
          publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
      }
    } else {
      val reply = write(UpdateRoomUserResponseBody(false))
      publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.UpdateRoomUserResponse.routingKey, exchange = "X:mounty-api-out")
    }
  }
}
