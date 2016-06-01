package kvstore

import akka.actor.{OneForOneStrategy, Props, ActorRef, Actor}
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val persistence = context.actorOf(persistenceProps)

  override def preStart() = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }
  context.system.scheduler.schedule(0.milliseconds, 100.milliseconds) {
    lastReq match {
      case Some((key, valueOption, id)) => {
        replicators.foreach {
          case r => {
            if (r == self) {
              persistence ! Persist(key, valueOption, id)
            } else {
              r ! Replicate(key, valueOption, id)
            }
          }
        }
      }
      case _ =>
    }
  }
  var lastReq = None :Option[(String, Option[String], Long)]
  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Replicas(replicas) => {
      secondaries.values.foreach {
        case r => context.stop(r)
      }
      secondaries = replicas.filter(_ != self).map(r => r -> context.actorOf(Replicator.props(r))).toMap
      replicators = secondaries.values.toSet // + self
    }
    case Replicated(key, id) => {
      lastReq match {
        case Some((key, valueOption, lid)) => {
          if (lid == id){
            replicators -= sender
          }
        }
        case _ =>
      }
    }
    case Persisted(key, id) => {
      replicators -= self
    }
    case Insert(key, value, id) => {
      kv += key -> value
      lastReq = Some((key, Some(value), id))
      replicators = secondaries.values.toSet + self
      val s = sender
      context.system.scheduler.scheduleOnce(1.seconds) {
        if (replicators.isEmpty) {
          s ! OperationAck(id)
        } else {
          s ! OperationFailed(id)
          replicators.empty
        }
      }
    }
    case Remove(key, id) => {
      kv -= key
      lastReq = Some((key, None, id))
      replicators = secondaries.values.toSet + self
      val s = sender
      context.system.scheduler.scheduleOnce(1.seconds) {
        if (replicators.isEmpty) {
          s ! OperationAck(id)
        } else {
          s ! OperationFailed(id)
          replicators.empty
        }
      }
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  var seq = 0L
  val replica: Receive = {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key, valueOption, id) => {
      if (id == seq) {
        valueOption match {
          case Some(value) => {
            kv += key -> value
          }
          case None => {
            kv -= key
          }
        }
        seq += 1
        val s = sender
        replicators += self
        lastReq = Some((key, valueOption, id))
        context.system.scheduler.scheduleOnce(1.seconds){
          if (replicators.isEmpty){
            s ! SnapshotAck(key, id)
          }else{
            replicators.empty
          }
        }
      } else if (id < seq) {
        sender ! SnapshotAck(key, id)
      }
    }
    case Persisted(key, id) => {
      replicators -= self
    }
    case _ =>
  }

}

