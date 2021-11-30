package services

import akka.actor.ActorRef
import akka.util.Timeout
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.MountyApi
import kz.mounty.fm.domain.requests.{UpdateRoomRequestBody, _}
import kz.mounty.fm.amqp.messages.MountyMessages.SpotifyGateway
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.{RoomUser, RoomUserType}
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ExceptionInfo, MountyException, ServerErrorRequestException}
import kz.mounty.fm.serializers.Serializers
import org.bson.conversions.Bson
import org.json4s.Formats
import org.json4s.jackson.Serialization._
import org.json4s.native.JsonMethods._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
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
                  userRoomCol: MongoCollection[RoomUser],
                  val timeout: Timeout) extends Serializers {

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

  def saveRoomsAndCreateRoomUser(amqpMessage: AMQPMessage, roomUserService: RoomUserService): Unit = {
    (for {
      response <- Future(parse(amqpMessage.entity).extract[GetCurrentUserRoomsGatewayResponseBody])
      separatedRooms <- separateSavedAndNotSavedRooms(response.rooms)
      _ <- Future.sequence(separatedRooms._2.map { room =>
        roomUserService.createRoomUserIfNotExist(response.userId, room.id, RoomUserType.CREATOR)
        roomRepository.create[Room](room)
      })
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

    def checkIfInviteCodeIsDefinedAndInUse(inviteCode: Option[String]): Future[Unit] = {
      if (inviteCode.isDefined) {
        roomRepository.findByFilter[Room](equal("inviteCode", inviteCode.get)).map {
          case Some(_) =>
            val error = ServerErrorRequestException(
              ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
              Some("this invite code is already in use")
            )
            throw error
          case None => //skip
        }
      } else Future {}
    }

    def prepare(request: UpdateRoomRequestBody): Future[Seq[Bson]] = Future {
      var updatedBson: Seq[Bson] = Seq()

      if (request.title.isDefined) updatedBson :+= set("title", request.title.get)
      if (request.isPrivate.isDefined) updatedBson :+= set("isPrivate", request.isPrivate.get)
      if (request.inviteCode.isDefined) updatedBson :+= set("inviteCode", request.inviteCode.get)
      if (request.isPrivate.isDefined && parsedRequest.isPrivate.get == false) updatedBson :+= set("inviteCode", null)
      if (request.imageUrl.isDefined) updatedBson :+= set("inviteCode", request.imageUrl.get)

      updatedBson
    }

    def process(updatedBson: Seq[Bson]): Future[Boolean] = {
      if (updatedBson.nonEmpty) {
        roomRepository.updateOneByFilter[Room](equal("id", parsedRequest.id), updatedBson)
      } else Future(false)
    }

    (for {
      _ <- checkIfInviteCodeIsDefinedAndInUse(parsedRequest.inviteCode)
      preparedBson <- prepare(parsedRequest)
      isUpdated <- process(preparedBson)
    } yield
      publisher ! amqpMessage.copy(
        entity = write(UpdateRoomResponseBody(isUpdated)),
        routingKey = MountyApi.UpdateRoomResponse.routingKey,
        exchange = "X:mounty-api-out")
      ) recover {
      case exception: Throwable =>
        val error = exception match {
          case e: MountyException => e.getExceptionInfo
          case _ =>
            ServerErrorRequestException(
              ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
              Some(exception.getMessage)
            ).getExceptionInfo
        }
        publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
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
