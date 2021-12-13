package actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import kz.mounty.fm.amqp.messages.MountyMessages.{MountyApi, RoomCore, SpotifyGateway}
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.domain.requests._
import kz.mounty.fm.domain.user.RoomUserType
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ServerErrorRequestException}
import kz.mounty.fm.serializers.Serializers
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import services.{PlayerService, RoomService, RoomUserService}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt


object AmqpListenerActor {
  def props()(implicit system: ActorSystem, ex: ExecutionContext, publisher: ActorRef, playerService: PlayerService, roomService: RoomService, roomUserService: RoomUserService): Props =
    Props(new AmqpListenerActor())
}

class AmqpListenerActor(implicit system: ActorSystem, ex: ExecutionContext, publisher: ActorRef, playerService: PlayerService, roomService: RoomService, roomUserService: RoomUserService)
  extends Actor
    with ActorLogging
    with Serializers {
  implicit val timeout: Timeout = 5.seconds

  override def receive: Receive = {
    case message: String =>
      log.info(s"received message $message")
      val amqpMessage = parse(message).extract[AMQPMessage]

      amqpMessage.routingKey match {

        case RoomCore.PauseSong.routingKey =>
          playerService.controlPlayer(amqpMessage)
        case RoomCore.PlayPrevTrack.routingKey =>
          playerService.controlPlayer(amqpMessage)
        case RoomCore.PlayNextTrack.routingKey =>
          playerService.controlPlayer(amqpMessage)
        case RoomCore.PlaySong.routingKey =>
          playerService.controlPlayer(amqpMessage)
        case RoomCore.PlayerPlayGatewayResponse.routingKey =>
          log.info("play pressed")
        case RoomCore.PlayerNextGatewayResponse.routingKey =>
          log.info("next song pressed")
        case RoomCore.PlayerPrevGatewayResponse.routingKey =>
          log.info("prev song pressed")
        case RoomCore.PlayerPauseGatewayResponse.routingKey =>
          log.info("pause song pressed")
        case RoomCore.GetRoomsForExploreRequest.routingKey =>
          roomService.getRoomsForExplore(amqpMessage)
        case RoomCore.GetRoomsAndRoomTracksRequest.routingKey =>
          val body = parse(amqpMessage.entity).extract[GetRoomAndRoomTracksRequestBody]
          publisher ! amqpMessage.copy(
            entity = write(GetPlaylistTracksGatewayRequestBody(playlistId = body.roomId, offset = body.offset, limit = body.limit, tokenKey = body.tokenKey)),
            routingKey = SpotifyGateway.GetPlaylistTracksGatewayRequest.routingKey,
            exchange = "X:mounty-spotify-gateway-in"
          )
        case RoomCore.GetPlaylistTracksGatewayResponse.routingKey =>
          roomService.getRoomAndRoomTracks(amqpMessage)
        case RoomCore.GetCurrentUserRoomsRequest.routingKey =>
          roomService.getCurrentUserRooms(amqpMessage)
        case RoomCore.GetCurrentUserRoomsGatewayResponse.routingKey =>
          roomService.saveRoomsAndCreateRoomUser(amqpMessage, roomUserService)
        case RoomCore.GetRoomByInviteCodeRequest.routingKey =>
          roomService.getRoomByInviteCode(amqpMessage)
        case RoomCore.UpdateRoomRequest.routingKey =>
          roomService.updateRoom(amqpMessage)
        case RoomCore.GetRoomUsersRequest.routingKey =>
          roomUserService.getRoomUsers(amqpMessage)
        case RoomCore.GetRoomUserByIdRequest.routingKey =>
          roomUserService.getRoomUserById(amqpMessage)
        case RoomCore.UpdateRoomUserRequest.routingKey =>
          roomUserService.updateRoomUser(amqpMessage)
        case RoomCore.CreateRoomUserIfNotExistRequest.routingKey =>
          val request = parse(amqpMessage.entity).extract[CreateRoomUserIfNotExistRequestBody]
          roomUserService.createRoomUserIfNotExist(request.profileId, request.roomId, RoomUserType.ORDINARY).map { roomUser =>
            publisher ! amqpMessage.copy(
              entity = write(CreateRoomUserIfNotExistResponseBody(roomUser)),
              routingKey = MountyApi.CreateRoomUserIfNotExistResponse.routingKey, exchange = "X:mounty-api-out")
          } recover {
            case exception: Throwable =>
              exception.printStackTrace()
              val error = ServerErrorRequestException(
                ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
                Some(exception.getCause.getMessage)
              ).getExceptionInfo
              publisher ! amqpMessage.copy(entity = write(error), routingKey = MountyApi.Error.routingKey, exchange = "X:mounty-api-out")
          }
        case RoomCore.GetCurrentlyPlayingTrackRequest.routingKey =>
          playerService.getCurrentlyPlayingTrack(amqpMessage)
        case RoomCore.GetCurrentlyPlayingTrackGatewayResponse.routingKey =>
          try {
            val gatewayResponse = parse(amqpMessage.entity).extract[GetCurrentlyPlayingTrackGatewayResponseBody]
            publisher ! amqpMessage.copy(entity = write(GetCurrentlyPlayingTrackResponseBody(Some(gatewayResponse.track))), routingKey = MountyApi.GetCurrentlyPlayingTrackResponse.routingKey, exchange = "X:mounty-api-out")
          } catch {
            case _: Throwable =>
              publisher ! amqpMessage.copy(entity = write(GetCurrentlyPlayingTrackResponseBody(None)), routingKey = MountyApi.GetCurrentlyPlayingTrackResponse.routingKey, exchange = "X:mounty-api-out")
          }
        case RoomCore.MakeRoomPrivateRequest.routingKey =>
          roomService.makeRoomPrivate(amqpMessage)
        case _ =>
          log.info("something else")
      }
  }
}
