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
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._
import spray.json._
import scala.collection.concurrent.TrieMap

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
 * Load balancer that routes activations by data proximity, falls back to round robin
 * Invokers may cache certain data. If an activation arrives with a data_dependency in its annotations, the invoker checks if that dependency exists
 * on any invoker, if so, the request gets routed to that invoker, otherwise, it follow a normal round robin approach.
 * 
 * Data dependencies are passed in under the "data_dependency" parameter. This should be the file name of the dependency.
 * The invoker will take the FullyQualifiedEntityName and append the data dependency file name to form the data dependency path.
 * This enables sharing of data within the same namespace.
 * [namespace]/[actionName]/[dataDependecyFileName]
 * 
 * Data dependency mappings are updated through health pings
 * 
 * TODO: Support file hashes
 * TODO: Support blackbox invokers (for now we will only support managed invokers)
 */
class DataProximityPoolBalancer(
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

  private var dataDepdencyFoundCounter = 0

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
    MetricEmitter.emitCounterMetric(INVOKER_DATA_DEPENDENCY_FOUND, dataDepdencyFoundCounter)
  }

  val schedulingState: DataProximityPoolBalancerState = DataProximityPoolBalancerState()()

  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)
      case InvokerLocalFiles(instance, localFiles) =>
        schedulingState.updateLocalFiles(instance, localFiles)
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

    val dataDependencyOpt: Option[String] = msg.content.flatMap {
      case JsObject(fields) => fields.get("data_dependency").collect { case JsString(s) => s }
      case _                => None
    }

    val chosen = if (invokersToUse.nonEmpty) {
      val start = Math.floorMod(rrCounter.getAndIncrement(), invokersToUse.size)
      val (invoker, dataDepdencyFound) = DataProximityPoolBalancer.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.invokerSlots,
        action.limits.memory.megabytes,
        schedulingState.fileToInvokers,
        action.fullyQualifiedName(false),
        dataDependencyOpt,
        start)
      if (dataDepdencyFound) {
        dataDepdencyFoundCounter += 1
        logging.info(this, s"data dependency found for activation ${action.fullyQualifiedName(true)} to invoker ${invoker.get._1} (data-proximity)")
      }
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
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB ($memoryLimitInfo), time limit ${timeLimit.duration.toMillis} ms ($timeLimitInfo) to $invoker (data-proximity)")
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

object DataProximityPoolBalancer extends LoadBalancerProvider {

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
    new DataProximityPoolBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  override def requiredProperties: Map[String, String] = kafkaHosts

  /**
   * Canonical key for a data-dependency entry in both the controller's
   * `fileToInvokers` index and the invoker's `localFiles` ping payload.
   * Both sides must call this helper to avoid silent drift.
   *
   * Format: `[namespace]/[actionName]/[fileName]`, e.g. `/guest/myAction/weights.bin`.
   */
  def dependencyKey(actionName: FullyQualifiedEntityName, fileName: String): String =
    s"${actionName.asString}/$fileName"

  /**
   * Tries invokers in order starting at `startIndex`, then overload random (same semantics as
   * [[ShardingContainerPoolBalancer.schedule]]).
   *
   * If `dataDependency` is set, first attempts invokers that already have the file cached
   * (intersected with `invokers` so partition/health invariants are preserved). Falls through
   * to the RR path if no such invoker can accept the activation.
   */
  def schedule(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int,
    fileToInvokers: TrieMap[String, Set[InvokerInstanceId]],
    actionName: FullyQualifiedEntityName,
    dataDependency: Option[String],
    startIndex: Int)(implicit logging: Logging, transId: TransactionId): (Option[(InvokerInstanceId, Boolean)], Boolean) = {

    dataDependency.foreach { dep =>
      val key = dependencyKey(actionName, dep)
      val ids = fileToInvokers.getOrElse(key, Set.empty[InvokerInstanceId])
      logging.info(this, s"activation has data dependency $key")
      logging.info(this, s"invoker ids: $ids")
      if (ids.nonEmpty) {
        // Intersect with the caller's partition (managed/blackbox) and filter by health.
        val it = invokers.iterator.filter(h => ids.contains(h.id) && h.status.isUsable)
        while (it.hasNext) {
          val h = it.next()
          if (dispatched(h.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
            return (Some((h.id, false)), true)
          }
        }
      }
    }

    val numInvokers = invokers.size
    if (numInvokers <= 0) (None, false)
    else {
      var step = 0
      while (step < numInvokers) {
        val index = Math.floorMod(startIndex + step, numInvokers)
        val invoker = invokers(index)
        if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
          return (Some((invoker.id, false)), false)
        }
        step += 1
      }
      val healthyInvokers = invokers.filter(_.status.isUsable)
      if (healthyInvokers.nonEmpty) {
        val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
        dispatched(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
        logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
        (Some((random, true)), false)
      } else (None, false)
    }
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _invokerSlots state of accessible slots of each invoker
 * @param _fileToInvokers map of file to invoker that has this file
 * @param _invokerToFiles map of invoker to files stored on that invoker
 */
case class DataProximityPoolBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  private var _clusterSize: Int = 1)(
  lbConfig: DataProximityPoolBalancerConfig =
    loadConfigOrThrow[DataProximityPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  // Concurrently accessed by the scheduler/routing path (readers) and updateLocalFiles (writer).
  // Readers are lock-free; the writer serializes via `synchronized` on this state instance to keep
  // the bidirectional indexes coherent with each other. Readers may observe partial updates.
  private val _fileToInvokers = TrieMap.empty[String, Set[InvokerInstanceId]]
  private val _invokerToFiles = TrieMap.empty[InvokerInstanceId, Set[String]]

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def clusterSize: Int = _clusterSize
  def fileToInvokers: TrieMap[String, Set[InvokerInstanceId]] = _fileToInvokers

  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  // Essentially just takes known invokers and partition them into managed/blackbox
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    // Drop data-proximity entries for invokers that are gone or Offline so the index
    // doesn't grow unboundedly and we don't keep routing to a restarted invoker whose
    // cache has been wiped. A recovered invoker will re-publish via its next ping.
    val liveIds: Set[InvokerInstanceId] =
      newInvokers.iterator.collect { case h if h.status != Offline => h.id }.toSet
    _invokerToFiles.keysIterator.toList.foreach { id =>
      if (!liveIds.contains(id)) updateLocalFiles(id, None)
    }

    val logDetail = if (oldSize != newSize) {
      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
        TransactionId.loadbalancer)
    }
  }

  def updateLocalFiles(instance: InvokerInstanceId, localFiles: Option[Seq[String]]): Unit = synchronized {
    val oldFileSet = _invokerToFiles.getOrElse(instance, Set.empty[String])
    val newFileSet = localFiles.map(_.toSet).getOrElse(Set.empty[String])

    if (oldFileSet == newFileSet) return

    val added = newFileSet.diff(oldFileSet)
    val removed = oldFileSet.diff(newFileSet)

    if (newFileSet.isEmpty) _invokerToFiles -= instance
    else _invokerToFiles(instance) = newFileSet

    removed.foreach { file =>
      _fileToInvokers.get(file).foreach { invokers =>
        val updated = invokers - instance
        if (updated.isEmpty) _fileToInvokers -= file
        else _fileToInvokers(file) = updated
      }
    }

    added.foreach { file =>
      val current = _fileToInvokers.getOrElse(file, Set.empty[InvokerInstanceId])
      _fileToInvokers(file) = current + instance
    }
  }
}

/**
 * Configuration for the sharding container pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 * @param timeoutAddon extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 */
case class DataProximityPoolBalancerConfig(managedFraction: Double,
                                               blackboxFraction: Double,
                                               timeoutFactor: Int,
                                               timeoutAddon: FiniteDuration)


