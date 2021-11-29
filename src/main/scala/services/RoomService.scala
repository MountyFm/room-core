package services

import akka.actor.ActorRef
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.MountyApi
import kz.mounty.fm.domain.requests._
import kz.mounty.fm.amqp.messages.MountyMessages.SpotifyGateway
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.{RoomUser, UserProfile}
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ExceptionInfo, ServerErrorRequestException}
import kz.mounty.fm.serializers.Serializers
import org.bson.conversions.Bson
import org.json4s.Formats
import org.json4s.jackson.Serialization._
import org.json4s.native.JsonMethods._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.{bitsAllSet, equal}
import repositories.RoomRepository
import scredis.Redis
import org.mongodb.scala.model.Updates.set

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class RoomService(implicit val redis: Redis,
                  publisher: ActorRef,
                  ex: ExecutionContext,
                  formats: Formats,
                  roomCol: MongoCollection[Room],
                  userRoomCol: MongoCollection[RoomUser]) extends Serializers {

  val roomRepository = new RoomRepository

  def getRoomsForExplore(amqpMessage: AMQPMessage) = {
    val size = parse(amqpMessage.entity).extract[GetRoomsForExploreRequestBody].size
    roomRepository.roomsBySizeAndFilter(size).onComplete {
      case Success(value) =>
        val body = GetRoomsForExploreResponseBody(value)
        publisher ! amqpMessage.copy(entity = write(body), exchange = "X:mounty-api-out", routingKey = MountyApi.GetRoomsForExploreResponse.routingKey)
      case Failure(exception) =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(exception.getMessage)
        ).getExceptionInfo
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def getRoomAndRoomTracks(amqpMessage: AMQPMessage) = {
    (for {
      body <- Future(parse(amqpMessage.entity).extract[GetPlaylistTracksGatewayResponseBody])
      room <- roomRepository.findByFilter[Room](equal("id", body.roomId))
    } yield {
      room match {
        case Some(room) =>
          val reply = write(GetRoomAndRoomTracksResponseBody(room, body.tracks))
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.GetRoomsAndRoomTracksResponse.routingKey, exchange = "X:mounty-api-out")
        case None =>
          val error = ServerErrorRequestException(
            ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
            Some("not found entity")
          ).getExceptionInfo
          val reply = write(error)
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
      }
    }) recover {
      case exception: Throwable =>
        exception match {
          case _: org.json4s.MappingException =>
            val exceptionInfo = parse(amqpMessage.entity).extract[ExceptionInfo]
            publisher ! amqpMessage.copy(entity = write(exceptionInfo), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
          case _ =>
            val error = ServerErrorRequestException(
              ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
              Some(exception.getMessage)
            ).getExceptionInfo
            publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
        }
    }
  }

  def getCurrentUserRooms(amqpMessage: AMQPMessage) = {
    val parsedRequest = parse(amqpMessage.entity).extract[GetCurrentUserRoomsRequestBody]
    val entity = GetCurrentUserRoomsGatewayRequestBody(parsedRequest.limit, parsedRequest.offset, parsedRequest.tokenKey)
    publisher ! amqpMessage.copy(entity = write(entity), routingKey = SpotifyGateway.GetCurrentUserRoomsGatewayRequest.routingKey, exchange = "X:mounty-spotify-gateway-in")
  }

  def saveRooms(amqpMessage: AMQPMessage): Unit = {
    (for {
      rooms <- Future(parse(amqpMessage.entity).extract[GetCurrentUserRoomsGatewayResponseBody].rooms)
      separatedRooms <- separateSavedAndNotSavedRooms(rooms)
      _ <- Future.sequence(separatedRooms._2.map(room => roomRepository.create[Room](room)))
    } yield
      publisher ! amqpMessage.copy(
        entity = write(GetCurrentUserRoomsResponseBody(separatedRooms._1 ++ separatedRooms._2)),
        routingKey = MountyApi.GetCurrentUserRoomsResponse.routingKey,
        exchange = "X:mounty-api-out")
      ) recover {
      case exception: Throwable =>
        exception match {
          case _: org.json4s.MappingException =>
            val exceptionInfo = parse(amqpMessage.entity).extract[ExceptionInfo]
            publisher ! amqpMessage.copy(entity = write(exceptionInfo), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
          case _ =>
            val error = ServerErrorRequestException(
              ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
              Some(exception.getMessage)
            ).getExceptionInfo
            publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
        }
    }
  }

  def getRoomByInviteCode(amqpMessage: AMQPMessage) = {
    val parsedRequest = parse(amqpMessage.entity).extract[GetRoomByInviteCodeRequestBody]

    roomRepository.findByFilter[Room](equal("inviteCode", parsedRequest.inviteCode))
      .map {
        case Some(room) =>
          val response = GetRoomByInviteCodeResponseBody(room = room)
          publisher ! amqpMessage.copy(entity = write(response), routingKey = MountyApi.GetRoomByInviteCodeResponse.routingKey, exchange = "X:mounty-api-out")
        case None =>
          val error = ServerErrorRequestException(
            ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
            Some("not found entity")
          ).getExceptionInfo
          publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
      } recover {
      case e: Throwable =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(e.getMessage)
        ).getExceptionInfo
        publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def updateRoom(amqpMessage: AMQPMessage) = {
    val parsedRequest = parse(amqpMessage.entity).extract[UpdateRoomRequestBody]

    var updatedBson: Seq[Bson] = Seq()
    if (parsedRequest.title.isDefined) updatedBson :+= set("title", parsedRequest.title.get)
    if (parsedRequest.isPrivate.isDefined) updatedBson :+= set("isPrivate", parsedRequest.isPrivate.get)
    if (parsedRequest.inviteCode.isDefined) updatedBson :+= set("inviteCode", parsedRequest.inviteCode.get)
    if (parsedRequest.imageUrl.isDefined) updatedBson :+= set("inviteCode", parsedRequest.imageUrl.get)

    if (updatedBson.nonEmpty) {
      roomRepository.updateOneByFilter[Room](equal("id", parsedRequest.id), updatedBson).onComplete {
        case Success(value) =>
          val reply = write(UpdateRoomResponseBody(value))
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.UpdateRoomResponse.routingKey, exchange = "X:mounty-api-out")
        case Failure(exception) =>
          val error = ServerErrorRequestException(
            ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
            Some(exception.getMessage)
          ).getExceptionInfo

          publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.UpdateRoomResponse.routingKey, exchange = "X:mounty-api-out")
      }
    } else {
      val reply = write(UpdateRoomResponseBody(false))
      publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.UpdateRoomResponse.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def separateSavedAndNotSavedRooms(rooms: Seq[Room]): Future[(Seq[Room], Seq[Room])] = {
    for {
      tuples <- Future.sequence {
        rooms.map { room =>
          roomRepository.findByFilter[Room](equal("id", room.id)).map {
            case Some(r) => (r, true)
            case None => (room, false)
          }
        }
      }
    } yield (tuples.filter(_._2 == true).map(_._1), tuples.filter(_._2 == false).map(_._1))
  }
}
