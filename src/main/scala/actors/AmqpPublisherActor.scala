package actors

import akka.actor.{Actor, ActorLogging, Props}
import com.rabbitmq.client.Channel
import kz.mounty.fm.amqp.AmqpPublisher
import kz.mounty.fm.amqp.messages.AMQPMessage
import kz.mounty.fm.serializers.Serializers

import scala.util.{Failure, Success}

object AmqpPublisherActor {
  def props(channel: Channel): Props =
    Props(new AmqpPublisherActor(channel))
}
class AmqpPublisherActor(channel: Channel)
  extends Actor
    with ActorLogging
    with Serializers{
  override def receive: Receive = {
    case message: AMQPMessage =>
      log.info(s"sending message: $message to AMQP")

      AmqpPublisher.publish(
        message,
        channel
      ) match {
        case Success(value) => log.info(s"succefully send message $message")
        case Failure(exception) => log.warning(s"couldn't message ${exception.getMessage}")

      }
  }
}