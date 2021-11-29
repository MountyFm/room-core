package services

import akka.actor.ActorRef
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.{MountyApi, RoomCore, SpotifyGateway}
import kz.mounty.fm.domain.commands._
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.{RoomUser, RoomUserType}
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ServerErrorRequestException}
import org.json4s.Formats
import org.json4s.jackson.Serialization._
import org.json4s.native.JsonMethods._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.{and, equal}
import repositories.{RoomRepository, RoomUserRepository}
import scredis.Redis

import scala.concurrent.{ExecutionContext, Future}

class PlayerService(implicit val redis: Redis,
                    publisher: ActorRef,
                    ex: ExecutionContext,
                    formats: Formats,
                    roomCol: MongoCollection[Room],
                    userRoomCol: MongoCollection[RoomUser] ) {

  val roomRepository = new RoomRepository()
  val roomUserRepository = new RoomUserRepository()

  def controlPlayer(amqpMessage: AMQPMessage): Future[Unit] = {
   val (tokenKey, roomId) = amqpMessage.routingKey match {
      case RoomCore.PauseSong.routingKey => (parse(amqpMessage.entity).extract[PauseSongCommandBody].tokenKey, parse(amqpMessage.entity).extract[PauseSongCommandBody].roomId)
      case RoomCore.PlayPrevTrack.routingKey => (parse(amqpMessage.entity).extract[PrevSongCommandBody].tokenKey, parse(amqpMessage.entity).extract[PrevSongCommandBody].roomId)
      case RoomCore.PlayNextTrack.routingKey => (parse(amqpMessage.entity).extract[NextSongCommandBody].tokenKey,parse(amqpMessage.entity).extract[NextSongCommandBody].roomId )
      case RoomCore.PlaySong.routingKey => (parse(amqpMessage.entity).extract[PlaySongCommandBody].tokenKey,parse(amqpMessage.entity).extract[PlaySongCommandBody].roomId)
    }

    (for {
      roomUsers <- roomUserRepository.findAllByFilter[RoomUser](equal("roomId", roomId))
      activeUsers <- Future(roomUsers.filter(_.`type` != RoomUserType.NOT_ACTIVE))
      accessTokens <- Future.sequence(activeUsers.map( user => redis.get(user.id)))
    } yield {
      accessTokens.foreach {
        case Some(value) =>
          amqpMessage.routingKey match {
            case RoomCore.PauseSong.routingKey =>
              val request = write(PlayerPauseGatewayCommandBody(deviceId = None, tokenKey = value))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPauseGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlayPrevTrack.routingKey =>
              val request = write(PlayerPrevGatewayCommandBody(deviceId = None, tokenKey = value))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPrevGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlayNextTrack.routingKey =>
              val request = write(PlayerNextGatewayCommandBody(deviceId = None, tokenKey = value))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerNextGatewayCommand.routingKey, amqpMessage.actorPath)

            case RoomCore.PlaySong.routingKey =>
              val body = parse(amqpMessage.entity).extract[PlaySongCommandBody]
              val request = write(PlayerPlayGatewayCommandBody(deviceId = None, tokenKey = value, contextUri = body.contextUri, offset = body.offset))
              publisher ! AMQPMessage(request, "X:mounty-spotify-gateway-in", SpotifyGateway.PlayerPlayGatewayCommand.routingKey, amqpMessage.actorPath)

          }
        case None =>
          val error = ServerErrorRequestException(
            ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
            Some("token not found")
          ).getMessage
          val reply = write(error)
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
      }
      amqpMessage.routingKey match {
        case RoomCore.PauseSong.routingKey =>
          publisher ! amqpMessage.copy(entity = write(PauseSongResponseBody()),routingKey = MountyApi.PauseSongResponse.routingKey, exchange =  "X:mounty-api-out")
        case RoomCore.PlayPrevTrack.routingKey =>
          publisher ! amqpMessage.copy( entity = write(PrevSongResponseBody()),routingKey = MountyApi.PlayPrevTrackResponse.routingKey, exchange =  "X:mounty-api-out")
        case RoomCore.PlayNextTrack.routingKey =>
          publisher ! amqpMessage.copy(entity = write(NextSongResponseBody()),routingKey = MountyApi.PlayNextTrackResponse.routingKey, exchange =  "X:mounty-api-out")
        case RoomCore.PlaySong.routingKey =>
          publisher ! amqpMessage.copy(entity = write(PlaySongResponseBody()),routingKey = MountyApi.PlaySongResponse.routingKey, exchange =  "X:mounty-api-out")
      }
    }
    ).recover {
      case e: Throwable =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(e.getMessage)
        ).getMessage
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange =  "X:mounty-api-out")
    }
  }


}
