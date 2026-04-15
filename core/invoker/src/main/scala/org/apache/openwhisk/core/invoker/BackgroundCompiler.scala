/*
Background compiler for WASM actions.

Runs a single daemon thread that pulls .wasm paths off a queue and
AOT-compiles them with `wasmtime compile`. Compiled artifacts are
stored as <sha256>.cwasm in `cacheDir`.

IMPORTANT: wasmtime compile parallelizes internally, so a single
compile thread is sufficient.

Files are registered as <sha256 of path>.cwasm

TODOs:
  - Cache eviction/sizing policies
  - Compilation should be done while there are no or few invocations active
  - Maybe look into using a Set to track compiled files rather than reading from disk
  - Only fetch binary if the file is not already present locally
*/

package org.apache.openwhisk.core.invoker

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.security.MessageDigest
import java.util.concurrent._
import scala.concurrent.duration._

import org.apache.openwhisk.common.Logging

class BackgroundCompiler(cacheDir: Path)(implicit logging: Logging) {
  private val actionsToCompile = new LinkedBlockingQueue[Path]()
  private val wasmtimeBinary = "wasmtime"
  private val compileTimeout = 60.seconds
  // private val inFlight = ConcurrentHashMap.newKeySet[String]()
  // private val invocationsActive = new atomic.AtomicInteger(0)

  // single compile thread
  private val worker = new Thread(() => runLoop(), "wasm-compiler")
  worker.setDaemon(true)
  // Set low priority to not interrupt other tasks
  worker.setPriority(Thread.NORM_PRIORITY - 1)
  worker.start()

  // periodic sweep, every 30s
  // This picks up fields that were not added to the queue (perhaps due to failure)
  // private val sweeper = Executors.newSingleThreadScheduledExecutor { r =>
  //   val t = new Thread(r, "wasm-sweeper"); t.setDaemon(true); t
  // }
  // sweeper.scheduleWithFixedDelay(() => sweep(), 30, 30, TimeUnit.SECONDS)

  def submit(wasm: Path): Unit = {
    val hash = sha256(wasm)
    // if (!Files.exists(cwasmFor(hash)) && inFlight.add(hash)) {
    if (!Files.exists(cwasmFor(hash))) {
      actionsToCompile.put(wasm)
      logging.info(this, s"[BackgroundCompiler] submitted ${wasm.getFileName} for compilation")
    }
  }

  // call from invocation handlers
  // def beginInvocation(): Unit = invocationsActive.incrementAndGet()
  // def endInvocation():   Unit = invocationsActive.decrementAndGet()
  def isCompiled(wasm: Path): Boolean = Files.exists(cwasmFor(sha256(wasm)))

  def compiledPath(wasm: Path): Path = cwasmFor(sha256(wasm))

  private def runLoop(): Unit =
    while (!Thread.currentThread.isInterrupted) {
      //   while (invocationsActive.get() > 0) Thread.sleep(20)  // yield to invocations
      // try compile(wasm) finally inFlight.remove(sha256(wasm))
      try {
        val wasm = actionsToCompile.take()
        compile(wasm)
      } catch {
        case _: InterruptedException => Thread.currentThread.interrupt()
        case e: Exception =>
          logging.error(this, s"[BackgroundCompiler] unexpected error: ${e.getMessage}")
      }
    }

  private def compile(wasm: Path): Unit = {
    val hash = sha256(wasm)
    if (Files.exists(cwasmFor(hash))) return

    val outPath = cwasmFor(hash).toString
    val command = Seq(wasmtimeBinary, "compile", "-o", outPath, wasm.toString)
    logging.info(this, s"[BackgroundCompiler] compiling ${wasm.getFileName}")

    val t0 = System.nanoTime()
    val pb = new ProcessBuilder(command: _*)
    pb.directory(cacheDir.toFile)
    pb.redirectErrorStream(true)
    val process = pb.start()
    process.getOutputStream.close()

    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    val output = new StringBuilder
    var line: String = null
    while ({ line = reader.readLine(); line != null }) output.append(line).append('\n')

    val finished = process.waitFor(compileTimeout.toMillis, TimeUnit.MILLISECONDS)
    val elapsedMs = (System.nanoTime() - t0) / 1e6

    if (!finished) {
      process.destroyForcibly()
      logging.error(this, s"[BackgroundCompiler] timed out after ${elapsedMs}ms compiling ${wasm.getFileName}")
    } else if (process.exitValue() != 0) {
      logging.error(this, s"[BackgroundCompiler] wasmtime failed (exit ${process.exitValue()}) for ${wasm.getFileName}: ${output.toString.take(500)}")
    } else {
      logging.info(this, s"[BackgroundCompiler] compiled ${wasm.getFileName} in ${elapsedMs.toLong}ms")
    }
  }

  private def cwasmFor(hash: String): Path = cacheDir.resolve(s"$hash.cwasm")

  // TODO: hash file content instead of path for correctness across renames
  private def sha256(p: Path): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val digest = md.digest(p.toString.getBytes(StandardCharsets.UTF_8))
    digest.map("%02x".format(_)).mkString
  }
}