package actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import kz.mounty.fm.amqp.messages.MountyMessages.RoomCore
import kz.mounty.fm.amqp.messages.{AMQPMessage, MountyMessages}
import kz.mounty.fm.serializers.Serializers
import org.json4s.native.JsonMethods.parse
import services.PlayerService

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt


object AmqpListenerActor {
  def props()(implicit system: ActorSystem, ex: ExecutionContext, publisher: ActorRef, playerService: PlayerService): Props =
    Props(new AmqpListenerActor())
}

class AmqpListenerActor(implicit system: ActorSystem, ex: ExecutionContext, publisher: ActorRef, playerService: PlayerService)
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
        case _ =>
          log.info("something else")
      }
  }
}
