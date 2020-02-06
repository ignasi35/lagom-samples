package com.example.shoppingcart.impl

import akka.NotUsed
import com.example.shoppingcart.api.ShoppingCartView
import com.example.shoppingcart.api.ShoppingCartItem
import com.example.shoppingcart.api.Quantity
import com.example.shoppingcart.api.ShoppingCartReport
import com.example.shoppingcart.api.ShoppingCartService
import com.example.shoppingcart.impl.ShoppingCart._
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.api.broker.Topic
import com.lightbend.lagom.scaladsl.api.transport.BadRequest
import com.lightbend.lagom.scaladsl.api.transport.NotFound
import com.lightbend.lagom.scaladsl.broker.TopicProducer
import com.lightbend.lagom.scaladsl.persistence.EventStreamElement
import com.lightbend.lagom.scaladsl.persistence.PersistentEntityRegistry

import scala.concurrent.ExecutionContext
import akka.cluster.sharding.typed.scaladsl.ClusterSharding

import scala.concurrent.duration._
import akka.util.Timeout
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.persistence.query.Offset
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.lightbend.lagom.scaladsl.persistence.AggregateEventShards
import com.lightbend.lagom.scaladsl.persistence.AggregateEventTag

import scala.collection.immutable

/**
 * Implementation of the `ShoppingCartService`.
 */
class ShoppingCartServiceImpl(
                               clusterSharding: ClusterSharding,
                               persistentEntityRegistry: PersistentEntityRegistry,
                               reportRepository: ShoppingCartReportRepository
                             )(implicit ec: ExecutionContext)
  extends ShoppingCartService {

  /**
   * Looks up the shopping cart entity for the given ID.
   */
  private def entityRef(id: String): EntityRef[Command] =
    clusterSharding.entityRefFor(ShoppingCart.typeKey, id)

  implicit val timeout = Timeout(5.seconds)

  override def get(id: String): ServiceCall[NotUsed, ShoppingCartView] = ServiceCall { _ =>
    entityRef(id)
      .ask(reply => Get(reply))
      .map(cartSummary => convertShoppingCart(id, cartSummary))
  }

  override def addItem(id: String): ServiceCall[ShoppingCartItem, ShoppingCartView] = ServiceCall { update =>
    entityRef(id)
      .ask(reply => AddItem(update.itemId, update.quantity, reply))
      .map { confirmation =>
        confirmationToResult(id, confirmation)
      }
  }

  override def removeItem(id: String, itemId: String): ServiceCall[NotUsed, ShoppingCartView] = ServiceCall { update =>
    entityRef(id)
      .ask(reply => RemoveItem(itemId, reply))
      .map { confirmation =>
        confirmationToResult(id, confirmation)
      }
  }

  override def adjustItemQuantity(id: String, itemId: String): ServiceCall[Quantity, ShoppingCartView] = ServiceCall {
    update =>
      entityRef(id)
        .ask(reply => AdjustItemQuantity(itemId, update.quantity, reply))
        .map { confirmation =>
          confirmationToResult(id, confirmation)
        }
  }

  override def checkout(id: String): ServiceCall[NotUsed, ShoppingCartView] = ServiceCall { _ =>
    entityRef(id)
      .ask(replyTo => Checkout(replyTo))
      .map { confirmation =>
        confirmationToResult(id, confirmation)
      }
  }

  private def confirmationToResult(id: String, confirmation: Confirmation): ShoppingCartView =
    confirmation match {
      case Accepted(cartSummary) => convertShoppingCart(id, cartSummary)
      case Rejected(reason) => throw BadRequest(reason)
    }

  override def shoppingCartTopic: Topic[ShoppingCartView] = {
    val userFlow =
      Flow[EventStreamElement[Event]]
        .filter(_.event.isInstanceOf[CartCheckedOut])
        .mapAsync(4) {
          case EventStreamElement(id, _, offset) =>
            entityRef(id)
              .ask(reply => Get(reply))
              .map(cart => convertShoppingCart(id, cart) -> offset)
        }
    topicSource.fromTags(Event.SingleTag) {
      userFlow
    }
    topicSource.fromTags( Event.ShardedTag) {
      userFlow
    }
  }

  val topicSource = TopicSourceFactory(persistentEntityRegistry)

  object TopicSourceFactory {
    def apply(registry: PersistentEntityRegistry): TopicSourceFactory = new TopicSourceFactory(registry)
  }

  private class TopicSourceFactory(registry: PersistentEntityRegistry) {
    /**
     * Given a `PersistentEntityRegistry` and a tag creates a stream processing the
     * journalled events via the `userFlow`. The streams processing the events are
     * distribuited across the cluster so Lagom ensures each event is not processed
     * in different nodes. The generated streams will use `at-least-once` semantics.
     */
    def fromTags(
              tag: AggregateEventTag[Event])(
              userFlow: Flow[EventStreamElement[Event], (ShoppingCartView, Offset), NotUsed]
            ): Topic[ShoppingCartView] =
      fromTaggedEntity(registry, Seq(tag))(userFlow)

    /**
     * Given a `PersistentEntityRegistry` and a collection of tags creates one stream
     * per tag processing the journalled events via the `userFlow`.The streams processing
     * the events are distribuited across the cluster so Lagom ensures each event is not
     * processed in different nodes. The generated streams will use `at-least-once` semantics.
     */
    def fromTags(
              tags: AggregateEventShards[Event])(
              userFlow: Flow[EventStreamElement[Event], (ShoppingCartView, Offset), NotUsed]
            ): Topic[ShoppingCartView] =
      fromTaggedEntity(registry, tags.allTags.toSeq)(userFlow)

    private def fromTaggedEntity(
                      registry: PersistentEntityRegistry,
                      tags: immutable.Seq[AggregateEventTag[Event]])(
                      userFlow: Flow[EventStreamElement[Event], (ShoppingCartView, Offset), NotUsed]
                    ): Topic[ShoppingCartView] = {
      TopicProducer.taggedStreamWithOffset(tags) {
        (tag, fromOffset) =>
          val journalSource: Source[EventStreamElement[Event], NotUsed] = registry
            .eventStream(tag, fromOffset)
          journalSource.via(userFlow)
      }
    }
  }

  private def convertShoppingCart(id: String, cartSummary: Summary) = {
    ShoppingCartView(
      id,
      cartSummary.items.map((ShoppingCartItem.apply _).tupled).toSeq,
      cartSummary.checkedOut
    )
  }

  override def getReport(cartId: String): ServiceCall[NotUsed, ShoppingCartReport] = ServiceCall { _ =>
    reportRepository.findById(cartId).map {
      case Some(cart) => cart
      case None => throw NotFound(s"Couldn't find a shopping cart report for $cartId")
    }
  }
}
