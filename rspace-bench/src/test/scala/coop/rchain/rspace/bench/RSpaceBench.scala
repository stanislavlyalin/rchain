package coop.rchain.rspace.bench

import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.effect.unsafe.{IORuntime, IORuntimeConfig}
import coop.rchain.metrics
import coop.rchain.metrics.{Metrics, NoopSpan, Span}
import coop.rchain.rholang.interpreter.RholangCLI
import coop.rchain.rspace.examples.AddressBookExample._
import coop.rchain.rspace.examples.AddressBookExample.implicits._
import coop.rchain.rspace.syntax._
import coop.rchain.rspace.util._
import coop.rchain.rspace.{RSpace, _}
import coop.rchain.shared.Log
import coop.rchain.shared.PathOps._
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import java.nio.file.Files
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

@org.openjdk.jmh.annotations.State(Scope.Thread)
trait RSpaceBenchBase {

  var space: ISpace[IO, Channel, Pattern, Entry, EntriesCaptor] = null

  val channel  = Channel("friends#" + 1.toString)
  val channels = List(channel)
  val matches  = List(CityMatch(city = "Crystal Lake"))
  val captor   = new EntriesCaptor()

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def measureAvgConsumeTime(bh: Blackhole) = {
    val r = space.consume(
      channels,
      matches,
      captor,
      persist = true
    )
    bh.consume(r)
  }

  def createIO(IOIndex: Int, iterations: Int): IO[Unit] =
    IO.delay {
      for (_ <- 1 to iterations) {
        val r1 = unpackOption(space.produce(channel, bob, persist = false).unsafeRunSync())
        runK(r1)
        getK(r1).results
      }
    }

  val IOsCount        = 200
  val iterationsCount = 10
  val IOs = (1 to IOsCount).map(idx => {
    val IO = createIO(idx, iterationsCount)
    IO
  })
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 0)
  @Threads(1)
  def simulateDupe(bh: Blackhole) = {

    val compute  = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))
    val sheduler = cats.effect.unsafe.Scheduler.createDefaultScheduler()

    space.consume(
      channels,
      matches,
      captor,
      persist = true
    )

    implicit val ior = IORuntime(compute, compute, sheduler._1, sheduler._2, IORuntimeConfig())

    val results: IndexedSeq[Future[Unit]] = IOs.map(f => f.unsafeToFuture()(ior))

    implicit val a = scala.concurrent.ExecutionContext.global
    bh.consume(Await.ready(Future.sequence(results), Duration.Inf))
  }
}

@org.openjdk.jmh.annotations.State(Scope.Thread)
@Warmup(iterations = 1)
@Fork(value = 2)
@Measurement(iterations = 10)
class RSpaceBench extends RSpaceBenchBase {

  implicit val logF: Log[IO]            = new Log.NOPLog[IO]
  implicit val noopMetrics: Metrics[IO] = new metrics.Metrics.MetricsNOP[IO]
  implicit val noopSpan: Span[IO]       = NoopSpan[IO]()

  val dbDir        = Files.createTempDirectory("rchain-rspace-bench-")
  val kvm          = RholangCLI.mkRSpaceStoreManager[IO](dbDir).unsafeRunSync()
  val rspaceStores = kvm.rSpaceStores

  import coop.rchain.shared.RChainScheduler._
  @Setup
  def setup() =
    space = RSpace
      .create[IO, Channel, Pattern, Entry, EntriesCaptor](rspaceStores.unsafeRunSync())
      .unsafeRunSync()

  @TearDown
  def tearDown() = {
    dbDir.recursivelyDelete()
    ()
  }
}
