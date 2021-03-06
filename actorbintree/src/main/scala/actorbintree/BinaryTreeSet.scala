/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op:Operation => root ! op
    case GC => {
      val newRoot = createRoot

      root ! CopyTo(newRoot)

      context.become(garbageCollecting(newRoot))

    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => pendingQueue :+= op
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot

      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]

      context.become(normal)
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(req, id, el) if (el == elem) => {
      removed = false
      req ! OperationFinished(id)
    }
    case Insert(req,id,el) if(el < elem&&subtrees.isDefinedAt(Left)) => subtrees(Left) ! Insert(req,id,el)
    case Insert(req,id,el) if(el > elem&&subtrees.isDefinedAt(Right)) => subtrees(Right) ! Insert(req,id,el)
    case Insert(req,id,el) => {
      if(el < elem){
        subtrees += Left -> context.actorOf(props(el,false))
        req ! OperationFinished(id)
      }else{
        subtrees += Right -> context.actorOf(props(el,false))
        req ! OperationFinished(id)
      }
    }
    case Remove(req, id, el) if (el == elem) => {
      removed = true
      req ! OperationFinished(id)
    }
    case Remove(req, id, el) if (el < elem && subtrees.isDefinedAt(Left)) => subtrees(Left) ! Remove(req,id,el)
    case Remove(req, id, el) if (el > elem && subtrees.isDefinedAt(Right)) => subtrees(Right) ! Remove(req,id,el)
    case Remove(req, id, el) => req ! OperationFinished(id)
    case Contains(req, id, el) if (el == elem) => req ! ContainsResult(id, !removed)
    case Contains(req, id, el) if (el < elem && subtrees.isDefinedAt(Left)) => subtrees(Left) ! Contains(req, id, el)
    case Contains(req, id, el) if (el > elem && subtrees.isDefinedAt(Right)) => subtrees(Right) ! Contains(req, id, el)
    case Contains(req, id, _) => req ! ContainsResult(id, false)
    case CopyTo(treeNode) => {
      val childSet = subtrees.values.toSet

      if(!removed) treeNode ! Insert(self,elem,elem)
      childSet.foreach(_ ! CopyTo(treeNode))
      if(removed&&childSet.isEmpty) sender ! CopyFinished
      else context.become(copying(childSet, removed))

    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => {

      if(expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      }else{
        context.become(copying(expected, true))
      }


    }
    case CopyFinished => {
      val newSet = expected - sender
      if (newSet.isEmpty && insertConfirmed){
        context.parent ! CopyFinished
        context.become(normal)
      }
      else context.become(copying(newSet, insertConfirmed))
    }
  }
}
