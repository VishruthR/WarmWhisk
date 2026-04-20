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

package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.event.Logging.InfoLevel
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest}
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.ack.{MessagingActiveAck, UserEventSender}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.v2.{InvokerHealthManager, NotSupportedPoolState, TotalContainerPoolState}
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.invoker.Invoker.InvokerEnabled
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._
import spray.json._
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.nio.file.Paths

object InvokerWASM extends InvokerProvider {
  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: IntraConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerWASM(config, instance, producer, poolConfig, limitsConfig)
}

class InvokerWASM(
  config: WhiskConfig,
  instance: InvokerInstanceId,
  producer: MessageProducer,
  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
  limitsConfig: IntraConcurrencyLimitConfig = loadConfigOrThrow[IntraConcurrencyLimitConfig](
    ConfigKeys.concurrencyLimit))(implicit actorSystem: ActorSystem, logging: Logging)
    extends InvokerCore {

  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val wasmtimeInvokeTimeout = 60.seconds

  /** Initialize needed databases */
  private val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, logging)

  private val authStore = WhiskAuthStore.datastore()

  private val namespaceBlacklist = new NamespaceBlacklist(authStore)

  private val currentPath = java.nio.file.Paths.get(".").toAbsolutePath.normalize.toString
  private val serveManager = new WasmtimeServeManager(Paths.get(currentPath))

  Scheduler.scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) { () =>
    logging.debug(this, "running background job to update blacklist")
    namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
      case Success(set) => logging.info(this, s"updated blacklist to ${set.size} entries")
      case Failure(t)   => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
    }
  }

  /** Initialize message consumers */
  private val topic = s"${Invoker.topicPrefix}invoker${instance.toInt}"
  private val maximumContainers = (poolConfig.userMemory / MemoryLimit.MIN_MEMORY).toInt
  private val msgProvider = SpiLoader.get[MessagingProvider]

  //number of peeked messages - increasing the concurrentPeekFactor improves concurrent usage, but adds risk for message loss in case of crash
  private val maxPeek =
    math.max(maximumContainers, (maximumContainers * limitsConfig.max * poolConfig.concurrentPeekFactor).toInt)

  private val consumer =
    msgProvider.getConsumer(config, topic, topic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("activation", logging, consumer, maxPeek, 1.second, processActivationMessage)
  })

  private val ack = {
    val sender = if (UserEvents.enabled) Some(new UserEventSender(producer)) else None
    new MessagingActiveAck(producer, instance, sender)
  }

  /** Stores an activation in the database. */
  private val store = (tid: TransactionId, activation: WhiskActivation, isBlocking: Boolean, context: UserContext) => {
    implicit val transid: TransactionId = tid
    activationStore.storeAfterCheck(activation, isBlocking, None, None, context)(tid, notifier = None, logging)
  }

  private def getActionFilename(actionFullyQualifiedName: String): String = {
    return actionFullyQualifiedName.replace('/', '_')
  }

  private def getActionPath(actionName: String): java.nio.file.Path = {
    Paths.get(currentPath).resolve(s"actions/${actionName}.wasm")
  }

  private def isBinaryPresent(actionName: String): Boolean = {
    val actionPath = getActionPath(actionName)
    java.nio.file.Files.exists(actionPath)
  }

  /**
   * Ensures the .wasm artifact for this action is materialized on disk, returning its path.
   * Uses an atomic-move pattern so concurrent writers don't corrupt the file.
   */
  private def materializeWasm(actionName: String, code: String): java.nio.file.Path = {
    val actionPath = getActionPath(actionName)
    if (!isBinaryPresent(actionName)) {
      val t0 = System.nanoTime()
      val bytes = java.util.Base64.getDecoder.decode(code)
      val tmp = java.nio.file.Files.createTempFile(actionPath.getParent, "wasm-", ".tmp")
      java.nio.file.Files.write(tmp, bytes)
      try java.nio.file.Files.move(tmp, actionPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
      catch { case _: java.nio.file.FileAlreadyExistsException => java.nio.file.Files.deleteIfExists(tmp) }
      val writeMs = (System.nanoTime() - t0) / 1e6
      logging.info(this, s"[wasm-timing] base64 decode + file write (${bytes.length} bytes) in ${writeMs}ms")
    }
    actionPath
  }

  // The invoker health probe uses a plain core-module `.wasm` (not a wasi:http/proxy
  // component), which `wasmtime serve` cannot run. Route it through the legacy
  // fork+exec `wasmtime run` path instead. Identified by the action name prefix
  // (e.g. "invokerHealthTestAction" or "invokerHealthTestActionWASM") from
  // `whisk.loadbalancer.invoker-health-test-action.name` in application.conf.
  private val healthActionNamePrefix: String = InvokerHealthManager.healthActionNamePrefix

  private def isHealthAction(actionName: String): Boolean =
    actionName.startsWith(healthActionNamePrefix)

  private def argToString(v: JsValue): String = v match {
    case JsString(s)  => s
    case JsNumber(n)  => n.toString
    case JsBoolean(b) => b.toString
    case JsNull       => ""
    case other        => other.compactPrint
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
    file.delete()
  }

  private def executeWithWasmtime(msg: ActivationMessage,
                                  executable: ExecutableWhiskAction): Future[(ActivationResponse, Instant, Instant)] = {
    // msg.content are the parameters passed to the action by the user at call time;
    // executable.parameters are the default parameters bound at action creation time.
    val params: JsObject = msg.content match {
      case Some(obj: JsObject) if obj.fields.nonEmpty => obj
      case _                                          => executable.parameters.toJsObject
    }

    val actionFileName = getActionFilename(executable.fullyQualifiedName(false).asString)

    def runWasm(actionPath: java.nio.file.Path): Future[(ActivationResponse, Instant, Instant)] = {
      // Health actions are plain core-module .wasm files that `wasmtime serve` cannot
      // host, so they always go through the fork+exec `wasmtime run` path.
      if (isHealthAction(executable.name.asString)) {
        executeWithWasmtimeRun(actionFileName, actionPath, params)
      } else {
        // Prefer a ready wasmtime-serve instance. If none is ready yet, start one
        // asynchronously (so it's warm for the next invocation) and handle this
        // activation via the fork+exec `wasmtime run` fallback.
        serveManager.tryGet(actionFileName) match {
          case Some(handle) =>
            logging.info(
              this,
              s"[wasmtime-serve] using ready server action=$actionFileName port=${handle.port}")
            executeWithWasmtimeServe(actionFileName, handle, params)
          case None =>
            logging.info(
              this,
              s"[wasmtime-serve] no ready server for action=$actionFileName; starting in background and falling back to wasmtime run")
            serveManager.startInBackground(actionFileName, actionPath)
            executeWithWasmtimeRun(actionFileName, actionPath, params)
        }
      }
    }

    executable.exec match {
      // Attachment was not fetched because the .wasm is already cached on disk.
      // Skip the inline-code path and run directly from the cached artifact.
      case CodeExecAsAttachment(_, _, _, binary) if binary && isBinaryPresent(actionFileName) =>
        runWasm(getActionPath(actionFileName))

      case CodeExecAsAttachment(_, Attachments.Inline(code), _, binary) if binary =>
        runWasm(materializeWasm(actionFileName, code))

      case _ =>
        val response = ActivationResponse.whiskError(
          s"action ${executable.fullyQualifiedName(false)} is not a valid WASM binary")
        val now = Instant.now
        Future.successful((response, now, now))
    }
  }

  private def executeWithWasmtimeServe(actionName: String,
                                       handle: WasmtimeServeHandle,
                                       params: JsObject): Future[(ActivationResponse, Instant, Instant)] = {
    MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_WASM_SERVE)
    val t0 = System.nanoTime()
    val started = Instant.now

    val tReq = System.nanoTime()
    val body = params.compactPrint
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"${handle.baseUrl}/",
      entity = HttpEntity(ContentTypes.`application/json`, body.getBytes(StandardCharsets.UTF_8)))

    Http().singleRequest(request).flatMap { resp =>
      resp.entity.toStrict(wasmtimeInvokeTimeout).map { strict =>
        val text = strict.data.utf8String
        val reqMs = (System.nanoTime() - tReq) / 1e6
        logging.info(
          this,
          s"[wasm-timing] http-request action=$actionName port=${handle.port} status=${resp.status.intValue} bytes=${text.length} in ${reqMs}ms")

        val response =
          if (resp.status.isSuccess()) {
            val json = Try(text.parseJson).getOrElse(JsObject("result" -> JsString(text.trim)))
            json match {
              case JsObject(fields) if fields.contains(ActivationResponse.ERROR_FIELD) =>
                ActivationResponse.applicationError(fields(ActivationResponse.ERROR_FIELD))
              case _ =>
                ActivationResponse.success(Some(json))
            }
          } else {
            ActivationResponse.developerError(
              s"wasmtime serve returned ${resp.status.intValue}: ${text.trim}")
          }

        val end = Instant.now
        val activationMs = java.time.Duration.between(started, end).toMillis
        val totalMs = (System.nanoTime() - t0) / 1e6
        logging.info(
          this,
          s"[wasm-timing] total=${totalMs}ms (httpRequest=${reqMs}ms, activation=${activationMs}ms)")
        (response, started, end)
      }
    }.recover {
      case t: Throwable =>
        val end = Instant.now
        logging.error(
          this,
          s"[wasmtime-serve] invocation failed action=$actionName: ${t.getClass.getName}: ${t.getMessage}")
        (ActivationResponse.whiskError(s"wasmtime serve failure: ${t.getMessage}"), started, end)
    }
  }

  /**
   * Backup execution path when wasmtime serve instance is starting up
   * Also used by InvokerHealthTestAction always
   */
  private def executeWithWasmtimeRun(actionName: String,
                                     actionPath: java.nio.file.Path,
                                     params: JsObject): Future[(ActivationResponse, Instant, Instant)] = {
    MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_WASM_RUN)
    // Sort params alphabetically and pass as positional CLI args (matches the
    // contract the old InvokerWASM used before wasmtime serve was introduced).
    val args = params.fields.toSeq.sortBy(_._1).map(p => argToString(p._2))

    Future {
      val t0 = System.nanoTime()
      val workDir = java.nio.file.Files.createTempDirectory("wasm-work-").toFile
      try {
        val started = Instant.now
        val t1 = System.nanoTime()
        val command = Seq("wasmtime", "-Scli", "-Shttp", "--dir", ".", actionPath.toString) ++ args
        logging.info(this, s"[wasm-run] executing action=$actionName cmd=${command.mkString(" ")}")

        val pb = new ProcessBuilder(command: _*)
        pb.directory(workDir)
        pb.redirectErrorStream(true)
        val process = pb.start()
        process.getOutputStream.close()
        val forkMs = (System.nanoTime() - t1) / 1e6
        logging.info(this, s"[wasm-timing] action=$actionName fork+exec wasmtime in ${forkMs}ms")

        val t2 = System.nanoTime()
        val output = scala.io.Source.fromInputStream(process.getInputStream, StandardCharsets.UTF_8.name()).mkString
        val readMs = (System.nanoTime() - t2) / 1e6
        logging.info(this, s"[wasm-timing] action=$actionName read stdout (${output.length} chars) in ${readMs}ms")

        val t3 = System.nanoTime()
        val finished = process.waitFor(wasmtimeInvokeTimeout.toMillis, TimeUnit.MILLISECONDS)
        val waitMs = (System.nanoTime() - t3) / 1e6
        logging.info(this, s"[wasm-timing] action=$actionName waitFor completed in ${waitMs}ms (finished=$finished)")

        val response =
          if (!finished) {
            process.destroyForcibly()
            ActivationResponse.whiskError(s"wasmtime timed out after ${wasmtimeInvokeTimeout.toSeconds} seconds")
          } else {
            val exit = process.exitValue()
            if (exit == 0) {
              val json = Try(output.parseJson).getOrElse(JsObject("result" -> JsString(output.trim)))
              json match {
                case JsObject(fields) if fields.contains(ActivationResponse.ERROR_FIELD) =>
                  ActivationResponse.applicationError(fields(ActivationResponse.ERROR_FIELD))
                case _ =>
                  ActivationResponse.success(Some(json))
              }
            } else {
              ActivationResponse.developerError(s"wasmtime exited with code $exit: ${output.trim}")
            }
          }

        val end = Instant.now
        val activationMs = java.time.Duration.between(started, end).toMillis
        val totalMs = (System.nanoTime() - t0) / 1e6
        logging.info(
          this,
          s"[wasm-timing] action=$actionName total=${totalMs}ms (fork=${forkMs}ms, readStdout=${readMs}ms, wait=${waitMs}ms, activation=${activationMs}ms)")

        (response, started, end)
      } finally {
        deleteRecursively(workDir)
      }
    }
  }

  private def buildActivation(msg: ActivationMessage,
                              executable: ExecutableWhiskAction,
                              response: ActivationResponse,
                              start: Instant,
                              end: Instant): WhiskActivation = {
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    val waitTime = Parameters(
      WhiskActivation.waitTimeAnnotation,
      Duration.fromNanos(java.time.Duration.between(msg.transid.meta.start, start).toNanos).toMillis.toJson)

    val binding =
      msg.action.binding.map(f => Parameters(WhiskActivation.bindingAnnotation, JsString(f.asString)))

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = executable.name,
      version = executable.version,
      start = start,
      end = end,
      duration = Some(java.time.Duration.between(start, end).toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, executable.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(executable.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(executable.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(false)) ++
          causedBy ++ binding ++ Some(waitTime)
      })
  }

  def handleActivationMessage(msg: ActivationMessage)(implicit transid: TransactionId): Future[Unit] = {
    val namespace = msg.action.path
    val name = msg.action.name
    val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
    val subject = msg.user.subject

    logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

    // caching is enabled since actions have revision id and an updated
    // action will not hit in the cache due to change in the revision id;
    // if the doc revision is missing, then bypass cache
    if (actionid.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionid.id}")

    val actionFilename = getActionFilename(FullyQualifiedEntityName(namespace, name).asString)
    WhiskAction
      .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty, fetchAttachment = !isBinaryPresent(actionFilename))
      .flatMap(action => {
        // action that exceed the limit cannot be executed.
        action.limits.checkLimits(msg.user)
        action.toExecutableWhiskAction match {
          case Some(executable) =>
            executeWithWasmtime(msg, executable).flatMap {
              case (response, start, end) =>
                val activation = buildActivation(msg, executable, response, start, end)
                activationFeed ! MessageFeed.Processed
                ack(
                  msg.transid,
                  activation,
                  msg.blocking,
                  msg.rootControllerIndex,
                  msg.user.namespace.uuid,
                  CombinedCompletionAndResultMessage(transid, activation, instance))
                store(msg.transid, activation, msg.blocking, UserContext(msg.user)).map(_ => ())
            }
          case None =>
            logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
            Future.failed(new IllegalStateException("non-executable action reached the invoker"))
        }
      })
      .recoverWith {
        case DocumentRevisionMismatchException(_) =>
          // if revision is mismatched, the action may have been updated,
          // so try again with the latest code
          handleActivationMessage(msg.copy(revision = DocRevision.empty))
        case t =>
          val response = t match {
            case _: NoDocumentException =>
              ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
            case e: ActionLimitsException =>
              ActivationResponse.applicationError(e.getMessage) // return generated failed message
            case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
              ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
            case e =>
              logging.error(this, s"unexpected error during activation ${msg.activationId}: ${e.getClass.getName}: ${e.getMessage}")
              ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
          }
          activationFeed ! MessageFeed.Processed

          val activation = generateFallbackActivation(msg, response)
          ack(
            msg.transid,
            activation,
            msg.blocking,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          store(msg.transid, activation, msg.blocking, UserContext(msg.user))
          Future.successful(())
      }
  }

  /** Is called when an ActivationMessage is read from Kafka */
  def processActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        // The message has been parsed correctly, thus the following code needs to *always* produce at least an
        // active-ack.

        implicit val transid: TransactionId = msg.transid

        //set trace context to continue tracing
        WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

        if (!namespaceBlacklist.isBlacklisted(msg.user)) {
          val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
          handleActivationMessage(msg)
        } else {
          // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
          // Due to the protective nature of the blacklist, a database entry is not written.
          activationFeed ! MessageFeed.Processed

          val activation =
            generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))
          ack(
            msg.transid,
            activation,
            false,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked in invoker.")
          Future.successful(())
        }
      }
      .recoverWith {
        case t =>
          // Iff everything above failed, we have a terminal error at hand. Either the message failed
          // to deserialize, or something threw an error where it is not expected to throw.
          activationFeed ! MessageFeed.Processed
          logging.error(this, s"terminal failure while processing message: $t")
          Future.successful(())
      }
  }

  /**
   * Generates an activation with zero runtime. Usually used for error cases.
   *
   * Set the kind annotation to `Exec.UNKNOWN` since it is not known to the invoker because the action fetch failed.
   */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.copy(version = None).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(Exec.UNKNOWN)) ++ causedBy
      })
  }

  private val healthProducer = msgProvider.getProducer(config)

  private def getHealthScheduler: ActorRef =
    Scheduler.scheduleWaitAtMost(1.seconds)(() => pingController(isEnabled = true))

  private def pingController(isEnabled: Boolean) = {
    healthProducer.send(s"${Invoker.topicPrefix}health", PingMessage(instance, isEnabled = Some(isEnabled))).andThen {
      case Failure(t) => logging.error(this, s"failed to ping the controller: $t")
    }
  }

  private var healthScheduler: Option[ActorRef] = Some(getHealthScheduler)

  override def enable(): String = {
    if (healthScheduler.isEmpty) {
      healthScheduler = Some(getHealthScheduler)
      s"${instance.toString} is now enabled."
    } else {
      s"${instance.toString} is already enabled."
    }
  }

  override def disable(): String = {
    pingController(isEnabled = false)
    if (healthScheduler.nonEmpty) {
      actorSystem.stop(healthScheduler.get)
      healthScheduler = None
      s"${instance.toString} is now disabled."
    } else {
      s"${instance.toString} is already disabled."
    }
  }

  override def isEnabled(): String = {
    InvokerEnabled(healthScheduler.nonEmpty).serialize()
  }

  override def backfillPrewarm(): String = {
    "not supported"
  }

  override def getPoolState(): Future[Either[NotSupportedPoolState, TotalContainerPoolState]] = {
    Future.successful(Left(NotSupportedPoolState()))
  }
}
