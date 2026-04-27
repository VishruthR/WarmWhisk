/*
Manages a pool of long-running `wasmtime serve` processes, one per action.

Lifecycle:
  - getOrStart(actionName, wasmPath) returns a Future[WasmtimeServeHandle],
    starting a new `wasmtime serve` subprocess if none exists for the action.
  - Concurrent callers for the same action share a single startup.
  - shutdown(actionName) stops the server (future eviction policies plug in here).
  - A JVM shutdown hook kills all spawned processes on normal exit/SIGTERM.

Port allocation:
  - We allocate from a pre-defined range [portRangeStart, portRangeEnd].
  - ASSUMPTION: these ports are free on the invoker host. If another process
    binds one first, the wasmtime serve that loses the race will fail its
    readiness check and the port will be returned to the pool.

Crash handling:
  - The JVM shutdown hook covers clean exits (normal termination, SIGTERM, SIGINT).
  - It does NOT cover SIGKILL or a hard JVM crash. In those cases wasmtime serve
    processes will be reparented to init and keep running.
  - TODO: wrap the subprocess with `setpriv --pdeathsig SIGTERM` (Linux) or
    equivalent PR_SET_PDEATHSIG to guarantee cleanup on any parent death.
*/

package org.apache.openwhisk.core.invoker

import java.io.{BufferedReader, InputStreamReader}
import java.net.{InetAddress, InetSocketAddress, Socket}
import java.nio.file.Path
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

import org.apache.openwhisk.common.Logging

final case class WasmtimeServeHandle(actionName: String,
                                     port: Int,
                                     process: Process,
                                     startedAtNs: Long) {
  def baseUrl: String = s"http://127.0.0.1:$port"
}

class WasmtimeServeManager(workDir: Path,
                           wasmtimeBinary: String = "wasmtime",
                           portRangeStart: Int = 18001,
                           portRangeEnd: Int = 18999,
                           readinessTimeout: FiniteDuration = 10.seconds,
                           readinessPollInterval: FiniteDuration = 20.millis)(implicit ec: ExecutionContext,
                                                                              logging: Logging) {

  private val availablePorts: ConcurrentLinkedQueue[Integer] = {
    val q = new ConcurrentLinkedQueue[Integer]()
    (portRangeStart to portRangeEnd).foreach(p => q.offer(Integer.valueOf(p)))
    q
  }

  private val servers = new ConcurrentHashMap[String, Future[WasmtimeServeHandle]]()

  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownAll(), "wasmtime-serve-shutdown"))

  /**
   * Returns a handle to a running wasmtime-serve process for the given action,
   * starting one if none exists. Concurrent calls for the same actionName share
   * the same startup.
   */
  def getOrStart(actionName: String, wasmPath: Path): Future[WasmtimeServeHandle] = {
    // Fast path: reuse an existing handle if the process is still alive.
    val existing = servers.get(actionName)
    if (existing != null) {
      existing.value match {
        case Some(Success(h)) if !h.process.isAlive =>
          logging.warn(this, s"[wasmtime-serve] '$actionName' process died; restarting")
          servers.remove(actionName, existing)
          availablePorts.offer(Integer.valueOf(h.port))
        case _ => // pending or alive
      }
    }

    val fut = servers.computeIfAbsent(
      actionName,
      _ => {
        val f = startServer(actionName, wasmPath)
        f.onComplete {
          case Failure(t) =>
            logging.error(this, s"[wasmtime-serve] startup for '$actionName' failed: ${t.getMessage}")
            servers.remove(actionName, f)
          case _ => ()
        }
        f
      })
    fut
  }

  /**
   * Non-blocking check for a ready server. Returns Some(handle) only if a server
   * has finished starting up and its process is still alive; returns None if no
   * server exists, startup is still in flight, or startup failed. Never triggers
   * a new startup.
   */
  def tryGet(actionName: String): Option[WasmtimeServeHandle] = {
    val f = servers.get(actionName)
    if (f == null) None
    else
      f.value match {
        case Some(Success(h)) if h.process.isAlive => Some(h)
        case _                                     => None
      }
  }

  /**
   * Kicks off startup for an action if no server is running or currently starting.
   * Returns immediately without waiting for readiness. Idempotent: concurrent calls
   * for the same actionName coalesce onto the same in-flight startup (via
   * `getOrStart` / `computeIfAbsent`).
   */
  def startInBackground(actionName: String, wasmPath: Path): Unit = {
    getOrStart(actionName, wasmPath)
    ()
  }

  /** Stops the server for a specific action (if any). Safe to call multiple times. */
  def shutdown(actionName: String): Future[Unit] = {
    val removed = servers.remove(actionName)
    if (removed == null) Future.successful(())
    else
      removed.transform { result =>
        result match {
          case Success(h) =>
            val t0 = System.nanoTime()
            h.process.destroy()
            if (!h.process.waitFor(2, TimeUnit.SECONDS)) h.process.destroyForcibly()
            availablePorts.offer(Integer.valueOf(h.port))
            val elapsedMs = (System.nanoTime() - t0) / 1e6
            logging.info(
              this,
              s"[wasmtime-serve] shut down '$actionName' on port ${h.port} in ${elapsedMs}ms")
          case Failure(_) => () // startup never succeeded; nothing to stop
        }
        Success(())
      }
  }

  /** Stops every running server. Called from the JVM shutdown hook. */
  def shutdownAll(): Unit = {
    val t0 = System.nanoTime()
    val keys = {
      val it = servers.keys()
      val buf = scala.collection.mutable.ArrayBuffer.empty[String]
      while (it.hasMoreElements) buf += it.nextElement()
      buf.toList
    }
    keys.foreach { k =>
      try { shutdown(k); () } catch { case _: Throwable => () }
    }
    val elapsedMs = (System.nanoTime() - t0) / 1e6
    logging.info(this, s"[wasmtime-serve] shutdownAll stopped ${keys.size} servers in ${elapsedMs}ms")
  }

  /** Snapshot of active action names (useful for eviction policies / debugging). */
  def activeActions: Set[String] = {
    import scala.collection.JavaConverters._
    servers.keys().asScala.toSet
  }

  private def startServer(actionName: String, wasmPath: Path): Future[WasmtimeServeHandle] = Future {
    blocking {
      val t0 = System.nanoTime()
      val portBoxed = availablePorts.poll()
      if (portBoxed == null) {
        throw new RuntimeException(
          s"wasmtime-serve port pool exhausted (range $portRangeStart-$portRangeEnd)")
      }
      val port = portBoxed.intValue()
      val addr = s"127.0.0.1:$port"
      val command = Seq(wasmtimeBinary, "serve", "-S", "cli=y", s"--dir=${workDir.toString}", "--addr", addr, wasmPath.toString)
      logging.info(
        this,
        s"[wasmtime-serve] spawning server action=$actionName port=$port cmd=${command.mkString(" ")}")

      val pb = new ProcessBuilder(command: _*)
      pb.directory(workDir.toFile)
      pb.redirectErrorStream(true)

      val process =
        try pb.start()
        catch {
          case t: Throwable =>
            availablePorts.offer(Integer.valueOf(port))
            throw t
        }
      process.getOutputStream.close()
      drainAsync(process, actionName)

      val spawnMs = (System.nanoTime() - t0) / 1e6
      logging.info(this, s"[wasmtime-serve] spawned action=$actionName pid=${process.pid()} in ${spawnMs}ms")

      val tReady = System.nanoTime()
      waitForReady(process, port, readinessTimeout) match {
        case Success(()) =>
          val readyMs = (System.nanoTime() - tReady) / 1e6
          val totalMs = (System.nanoTime() - t0) / 1e6
          logging.info(
            this,
            s"[wasmtime-serve] ready action=$actionName port=$port ready=${readyMs}ms total=${totalMs}ms")
          WasmtimeServeHandle(actionName, port, process, t0)
        case Failure(t) =>
          process.destroyForcibly()
          availablePorts.offer(Integer.valueOf(port))
          throw new RuntimeException(
            s"wasmtime serve for '$actionName' failed to become ready on $addr: ${t.getMessage}",
            t)
      }
    }
  }

  private def waitForReady(process: Process, port: Int, timeout: FiniteDuration): Try[Unit] = {
    val deadline = System.nanoTime() + timeout.toNanos
    var lastError: Throwable = null
    while (System.nanoTime() < deadline) {
      if (!process.isAlive) {
        return Failure(
          new RuntimeException(s"wasmtime serve exited before becoming ready (exit=${process.exitValue()})"))
      }
      try {
        val sock = new Socket()
        try {
          sock.connect(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), 200)
          return Success(())
        } finally sock.close()
      } catch {
        case e: Throwable =>
          lastError = e
          Thread.sleep(readinessPollInterval.toMillis)
      }
    }
    Failure(Option(lastError).getOrElse(new RuntimeException(s"timed out waiting for port $port")))
  }

  /**
   * Drains stdout (which includes stderr due to redirectErrorStream) so the OS
   * pipe buffer does not fill and block wasmtime. Lines are forwarded to the logger.
   */
  private def drainAsync(process: Process, actionName: String): Unit = {
    val t = new Thread(
      () => {
        val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
        try {
          var line: String = reader.readLine()
          while (line != null) {
            logging.info(this, s"[wasmtime-serve:$actionName] $line")
            line = reader.readLine()
          }
        } catch { case _: Throwable => () } finally {
          try reader.close()
          catch { case _: Throwable => () }
        }
      },
      s"wasmtime-serve-drain-$actionName")
    t.setDaemon(true)
    t.start()
  }
}
