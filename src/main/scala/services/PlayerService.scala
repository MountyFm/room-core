package services

import akka.actor.ActorRef
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.{MountyApi, RoomCore, SpotifyGateway}
import kz.mounty.fm.domain.commands._
import kz.mounty.fm.domain.requests.{GetCurrentlyPlayingTrackGatewayRequestBody, GetCurrentlyPlayingTrackGatewayResponseBody, GetCurrentlyPlayingTrackRequestBody, GetCurrentlyPlayingTrackResponseBody}
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.RoomUser
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ExceptionInfo, ServerErrorRequestException}
import org.json4s.Formats
import org.json4s.jackson.Serialization._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import repositories.{RoomRepository, RoomUserRepository}
import scredis.Redis
import org.json4s.jackson.JsonMethods._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PlayerService(implicit val redis: Redis,
                    publisher: ActorRef,
                    ex: ExecutionContext,
                    formats: Formats,
                    roomCol: MongoCollection[Room],
                    userRoomCol: MongoCollection[RoomUser]) {

  val roomRepository = new RoomRepository()
  val roomUserRepository = new RoomUserRepository()

  def controlPlayer(amqpMessage: AMQPMessage): Future[Unit] = {
    val (tokenKey, roomId) = amqpMessage.routingKey match {
      case RoomCore.PauseSong.routingKey => (parse(amqpMessage.entity).extract[PauseSongCommandBody].tokenKey, parse(amqpMessage.entity).extract[PauseSongCommandBody].roomId)
      case RoomCore.PlayPrevTrack.routingKey => (parse(amqpMessage.entity).extract[PrevSongCommandBody].tokenKey, parse(amqpMessage.entity).extract[PrevSongCommandBody].roomId)
      case RoomCore.PlayNextTrack.routingKey => (parse(amqpMessage.entity).extract[NextSongCommandBody].tokenKey, parse(amqpMessage.entity).extract[NextSongCommandBody].roomId)
      case RoomCore.PlaySong.routingKey => (parse(amqpMessage.entity).extract[PlaySongCommandBody].tokenKey, parse(amqpMessage.entity).extract[PlaySongCommandBody].roomId)
    }

    (for {
      roomUsers <- roomUserRepository.findAllByFilter[RoomUser](equal("roomId", roomId))
      activeUsers <- Future(roomUsers.filter(_.isActive == true))
    } yield {

      Future.sequence(
        activeUsers.map(user => Future{
          amqpMessage.routingKey match {
            case RoomCore.PauseSong.routingKey =>
              val request = write(PlayerPauseGatewayCommandBody(deviceId = None, tokenKey = user.profileId))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPauseGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlayPrevTrack.routingKey =>
              val request = write(PlayerPrevGatewayCommandBody(deviceId = None, tokenKey = user.profileId))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPrevGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlayNextTrack.routingKey =>
              val request = write(PlayerNextGatewayCommandBody(deviceId = None, tokenKey = user.profileId))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerNextGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlaySong.routingKey =>
              val body = parse(amqpMessage.entity).extract[PlaySongCommandBody]
              val request = write(PlayerPlayGatewayCommandBody(deviceId = None, tokenKey = user.profileId, contextUri = body.contextUri, offset = body.offset, positionMs = body.positionMs))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPlayGatewayCommand.routingKey, amqpMessage.actorPath)
          }
        })
      ).onComplete {
        case Success(value) =>
          println("Done")
        case Failure(exception) => println(exception.getMessage)
      }

      amqpMessage.routingKey match {
        case RoomCore.PauseSong.routingKey =>
          publisher ! amqpMessage.copy(entity = write(PauseSongResponseBody()), routingKey = MountyApi.PauseSongResponse.routingKey, exchange = "X:mounty-api-out")
        case RoomCore.PlayPrevTrack.routingKey =>
          publisher ! amqpMessage.copy(entity = write(PrevSongResponseBody()), routingKey = MountyApi.PlayPrevTrackResponse.routingKey, exchange = "X:mounty-api-out")
        case RoomCore.PlayNextTrack.routingKey =>
          publisher ! amqpMessage.copy(entity = write(NextSongResponseBody()), routingKey = MountyApi.PlayNextTrackResponse.routingKey, exchange = "X:mounty-api-out")
        case RoomCore.PlaySong.routingKey =>
          publisher ! amqpMessage.copy(entity = write(PlaySongResponseBody()), routingKey = MountyApi.PlaySongResponse.routingKey, exchange = "X:mounty-api-out")
      }
    }
      ).recover {
      case e: Throwable =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(e.getMessage)
        ).getMessage
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }

  def getCurrentlyPlayingTrack(amqpMessage: AMQPMessage): Unit = {
    val tokenKey = parse(amqpMessage.entity).extract[GetCurrentlyPlayingTrackRequestBody].tokenKey
    publisher ! amqpMessage.copy(entity = write(GetCurrentlyPlayingTrackGatewayRequestBody(tokenKey)), routingKey = SpotifyGateway.GetCurrentlyPlayingTrackGatewayRequest.routingKey, exchange = "X:mounty-spotify-gateway-in")
  }

  def handleGetCurrentlyPlayingTrackGatewayResponse(amqpMessage: AMQPMessage): Unit = {
    try {
      val gatewayResponse = parse(amqpMessage.entity).extract[GetCurrentlyPlayingTrackGatewayResponseBody]
      publisher ! amqpMessage.copy(entity = write(GetCurrentlyPlayingTrackResponseBody(gatewayResponse.track, gatewayResponse.progressMs)), routingKey = MountyApi.GetCurrentlyPlayingTrackResponse.routingKey, exchange = "X:mounty-api-out")
    } catch {
      case _: org.json4s.MappingException =>
        val exceptionInfo = parse(amqpMessage.entity).extract[ExceptionInfo]
        publisher ! amqpMessage.copy(entity = write(exceptionInfo), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
      case _ =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some("something went wrong while getting currently playing track")
        ).getExceptionInfo
        publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
    }
  }
}
