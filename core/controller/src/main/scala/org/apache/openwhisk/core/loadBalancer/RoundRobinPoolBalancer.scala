/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.loadBalancer

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pekko.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import org.apache.pekko.cluster.ClusterEvent._
import org.apache.pekko.cluster.{Cluster, Member, MemberStatus}
import org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap
import org.apache.pekko.management.scaladsl.PekkoManagement
import org.apache.openwhisk.common.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.controller.Controller
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.Future

/**
 * Load balancer that assigns activations with a global round-robin order over invokers (per managed / blackbox pool).
 *
 * Reuses [[ShardingContainerPoolBalancerState]] for invoker partitioning (managed vs blackbox fractions), per-invoker
 * slot accounting ([[NestedSemaphore]]), and cluster-based memory sharding — identical to
 * [[ShardingContainerPoolBalancer]].
 *
 * Scheduling: each activation picks a starting index with `nextIndex % N` (atomically incremented), then probes
 * invokers `(start, start+1, …)` modulo `N` for the first usable invoker with capacity. If none, uses the same
 * overload strategy as the sharding balancer (random healthy invoker + `forceAcquireConcurrent`).
 *
 * Unlike hash-based sharding, the same action is not pinned to a home invoker; warm-container affinity is weaker but
 * load is spread evenly when invokers are symmetric.
 */
class RoundRobinPoolBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    PekkoManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("pekko.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  private val rrManaged = new AtomicInteger(0)
  private val rrBlackbox = new AtomicInteger(0)

  override protected def emitMetrics(): Unit = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) curr.id.userMemory.toMB + total else total
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) curr.id.userMemory.toMB + total else total
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  val schedulingState: ShardingContainerPoolBalancerState = ()(lbConfig)

  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }
        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, rrCounter) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, rrManaged)
      else (schedulingState.blackboxInvokers, rrBlackbox)

    val chosen = if (invokersToUse.nonEmpty) {
      val start = Math.floorMod(rrCounter.getAndIncrement(), invokersToUse.size)
      val invoker: Option[(InvokerInstanceId, Boolean)] = RoundRobinPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        start)
      invoker.foreach {
        case (_, true) =>
          val metric =
            if (isBlackboxInvocation) LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      invoker.map(_._1)
    } else None

    chosen
      .map { invoker =>
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB ($memoryLimitInfo), time limit ${timeLimit.duration.toMillis} ms ($timeLimitInfo) to $invoker (round-robin)")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }
        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool: ActorRef =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }
}

object RoundRobinPoolBalancer extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[ResultMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        logging.info(this, s"preparing invoker pool including health test action $instance")
        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(
              whiskConfig,
              s"${Controller.topicPrefix}health${instance.asString}",
              s"${Controller.topicPrefix}health",
              maxPeek = 128),
            monitor))
      }
    }
    new RoundRobinPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  override def requiredProperties: Map[String, String] = kafkaHosts

  /**
   * Tries invokers in order starting at `startIndex`, then overload random (same semantics as
   * [[ShardingContainerPoolBalancer.schedule]]).
   */
  def schedule(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int,
    startIndex: Int)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
    val numInvokers = invokers.size
    if (numInvokers <= 0) None
    else {
      var step = 0
      while (step < numInvokers) {
        val index = Math.floorMod(startIndex + step, numInvokers)
        val invoker = invokers(index)
        if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
          return Some((invoker.id, false))
        }
        step += 1
      }
      if (step == numInvokers) {
        val healthyInvokers = invokers.filter(_.status.isUsable)
        if (healthyInvokers.nonEmpty) {
          val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
          dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
          logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
          Some((random, true))
        } else None
      } else None
    }
  }
}
