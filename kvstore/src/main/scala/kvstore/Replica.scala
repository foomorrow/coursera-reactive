package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replica.{OperationFailed, OperationAck}
import akka.pattern.{ ask, pipe }
import akka.util.{Timeout}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success


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
  val persistence = context.actorOf(persistenceProps)
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  //id -> cancelable
  var cancels = Map.empty[Long,Cancellable]
  var expectSeq = 0L
  arbiter ! Join
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }
  def doOperation(mySender:ActorRef,key:String,vo:Option[String],id:Long) = {
    vo match {
      case Some(value) => {
        kv += key -> value
      }
      case None => {
        kv -= key
      }
    }
    val mySender = sender
    replicators += sender

    val cancelable = context.system.scheduler.schedule(0.milliseconds,100.milliseconds,persistence, Persist(key,vo,id))
    cancels += id -> cancelable
    val allSnapShot = secondaries.values.map{replicator =>
      ask(replicator,Replicate(key,vo,id))(Timeout(1 seconds))
//      replicator ? (SnapshotAck(key,id))(Timeout(1.5 seconds))
    }
    context.system.scheduler.scheduleOnce(1.5 seconds){
      val allDone = allSnapShot.forall(f => {
        println(73,f.value)
        f.value match {
          case Some(t) => {
            t match {
              case Success(r) => {
                r match {
                  case SnapshotAck => true
                  case _ => false
                }
              }
              case _ => false
            }
          }
          case _ => false
        }
      })
//      println(74,allDone,allSnapShot)
      cancels.get(id) match {
        case Some(c) => {
          if(c.isCancelled){
            if(allDone) mySender ! OperationAck(id)
            else mySender ! OperationFailed(id)
          }else{
            mySender ! OperationFailed(id)
            c.cancel()
          }
        }
      }

      cancels-= id
    }
  }
  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key,value,id) => {
      doOperation(sender,key,Some(value),id)
    }
    case Remove(key,id) => {
      doOperation(sender,key,None,id)
    }
    case Get(key,id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Persisted(key,id) => {
      cancels.get(id) match{
        case Some(c) => c.cancel()
      }



    }
    case Replicas(replicas) => {
      replicas foreach(r => {
        if(r != self){
          secondaries += r -> context.actorOf(Replicator.props(r))
        }
      })
    }

    case OperationFailed(id)=>{

    }
  }
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key,id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key,valueOption,seq) if(seq < expectSeq) => {
      sender ! SnapshotAck(key,seq)
    }
    case Snapshot(key,valueOption,seq) if(seq == expectSeq || seq - expectSeq < 1L) => {
      valueOption match {
        case Some(v) => {
          kv += key -> v
        }
        case None => {
          kv -= key
        }
      }
      val mySender = sender
      replicators += sender
//      val future = doPersist(key,valueOption,seq)
//      future.onSuccess{
//        case a => println(137,a)
//      }
//      future.onFailure{
//        case a => {
//          if(!ended) doPersist(key,valueOption,seq)
//        }
//      }
      val cancelable = context.system.scheduler.schedule(0.milliseconds,100.milliseconds,persistence,Persist(key,valueOption,seq))
      cancels += seq -> cancelable
      context.system.scheduler.scheduleOnce(1.seconds){
        replicators -= mySender
        if (!cancelable.isCancelled) cancelable.cancel()
        cancels -= seq
      }
    }
    case Persisted(key,id) => {
      if(id == expectSeq)expectSeq += 1
      else expectSeq = id
      replicators.foreach(r => r ! SnapshotAck(key, id))
      cancels.get(id) match {
        case Some(s)  => {
          if (!s.isCancelled) s.cancel()

        }
      }
      cancels -= id
    }
    case Replicated(key,id) => {

    }
  }

}

