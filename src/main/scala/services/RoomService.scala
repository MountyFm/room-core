package services

import akka.actor.ActorRef
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.amqp.messages.MountyMessages.MountyApi
import kz.mounty.fm.domain.requests.{GetPlaylistTracksGatewayResponseBody, GetRoomAndRoomTracksResponseBody, GetRoomsForExploreRequestBody, GetRoomsForExploreResponseBody}
import kz.mounty.fm.domain.room.Room
import kz.mounty.fm.domain.user.RoomUser
import kz.mounty.fm.exceptions.{ErrorCodes, ErrorSeries, ServerErrorRequestException}
import org.json4s.Formats
import org.json4s.jackson.Serialization._
import org.json4s.native.JsonMethods._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import repositories.RoomRepository
import scredis.Redis

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class RoomService(implicit val redis: Redis,
                  publisher: ActorRef,
                  ex: ExecutionContext,
                  formats: Formats,
                  roomCol: MongoCollection[Room],
                  userRoomCol: MongoCollection[RoomUser]) {

  val roomRepository = new RoomRepository

  def getRoomsForExplore(amqpMessage: AMQPMessage) = {
    val size = parse(amqpMessage.entity).extract[GetRoomsForExploreRequestBody].size
    roomRepository.roomsBySizeAndFilter(size).onComplete {
      case Success(value) =>
        val body = GetRoomsForExploreResponseBody(value)
        publisher ! amqpMessage.copy(entity = write(body),exchange =  "X:mounty-api-out", routingKey = MountyApi.GetRoomsForExploreResponse.routingKey)
      case Failure(exception) =>
        val error = ServerErrorRequestException(
          ErrorCodes.INTERNAL_SERVER_ERROR(ErrorSeries.ROOM_CORE),
          Some(exception.getMessage)
        )
        val reply = write(error)
        publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange =  "X:mounty-api-out")
    }
  }

  def getRoomAndRoomTracks(amqpMessage: AMQPMessage) = {
    val body = parse(amqpMessage.entity).extract[GetPlaylistTracksGatewayResponseBody]
    (for {
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
          )
          val reply = write(error)
          publisher ! amqpMessage.copy(entity = reply, routingKey = MountyApi.Error.routingKey, exchange =  "X:mounty-api-out")
      }
    })
  }
}
