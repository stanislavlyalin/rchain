package coop.rchain.rholang

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import coop.rchain.metrics.{Metrics, NoopSpan, Span}
import coop.rchain.models.Expr.ExprInstance.{GInt, GString}
import coop.rchain.models.rholang.implicits._
import coop.rchain.shared.Log
import coop.rchain.rholang.interpreter.InterpreterUtil
import coop.rchain.shared.RChainScheduler._

import scala.concurrent.duration._

class PeekSpec extends AnyFlatSpec with Matchers {

  import Resources._
  import InterpreterUtil._

  implicit val logF: Log[IO]            = new Log.NOPLog[IO]
  implicit val noopMetrics: Metrics[IO] = new Metrics.MetricsNOP[IO]
  implicit val noopSpan: Span[IO]       = NoopSpan[IO]()

  val tmpPrefix = "peek-spec-"

  "peek" should "not remove read data" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _    <- evaluate[IO](runtime, """@1!("v1") | for(_ <<- @1) { Nil }""")
        _    <- evaluate[IO](runtime, """for(_ <- @1) { @2!("v2") }""")
        data <- runtime.getData(GInt(2L))
        _ = withClue(
          "Continuation didn't produce expected data. Did it fire?"
        ) { data should have size 1 }
      } yield (data.head.a.pars.head.exprs.head.exprInstance shouldBe GString("v2"))
    }.unsafeRunSync
  }

  it should "not duplicate read persistent data - send is executed first" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _          <- evaluate[IO](runtime, """@1!!("v1")""")
        _          <- evaluate[IO](runtime, """for(_ <<- @1) { Nil }""")
        _          <- evaluate[IO](runtime, """for(_ <- @1) { @2!("v2") }""")
        v1Data     <- runtime.getData(GInt(1L))
        _          = v1Data should have size 1
        resultData <- runtime.getData(GInt(2L))
        _ = withClue(
          "Continuation didn't produce expected data. Did it fire?"
        ) { resultData should have size 1 }
      } yield (resultData.head.a.pars.head.exprs.head.exprInstance shouldBe GString("v2"))
    }.unsafeRunSync
  }

  it should "not duplicate read persistent data - send is executed second" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _          <- evaluate[IO](runtime, """for(_ <<- @1) { Nil }""")
        _          <- evaluate[IO](runtime, """@1!!("v1")""")
        _          <- evaluate[IO](runtime, """for(_ <- @1) { @2!("v2") }""")
        v1Data     <- runtime.getData(GInt(1L))
        _          = v1Data should have size 1
        resultData <- runtime.getData(GInt(2L))
        _ = withClue(
          "Continuation didn't produce expected data. Did it fire?"
        ) { resultData should have size 1 }
      } yield (resultData.head.a.pars.head.exprs.head.exprInstance shouldBe GString("v2"))
    }.unsafeRunSync
  }

  it should "clear all peeks when inserting a persistent send" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _   <- evaluate[IO](runtime, """for (_ <<- @0) { @1!(0) }""")
        _   <- evaluate[IO](runtime, """for (_ <<- @0) { @1!(0) }""")
        _   <- evaluate[IO](runtime, """@0!!(0)""")
        res <- runtime.getData(GInt(1L)).map(_.size)
      } yield (res shouldBe 2)
    }.unsafeRunSync
  }

  it should "clear all peeks when inserting a send" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _   <- evaluate[IO](runtime, """for (_ <<- @0) { @1!(0) }""")
        _   <- evaluate[IO](runtime, """for (_ <<- @0) { @1!(0) }""")
        _   <- evaluate[IO](runtime, """@0!(0)""")
        res <- runtime.getData(GInt(1L)).map(_.size)
      } yield (res shouldBe 2)
    }.unsafeRunSync
  }

  it should "continue executing the loop until quiescence" in {
    mkRuntime[IO](tmpPrefix).use { runtime =>
      for {
        _  <- evaluate[IO](runtime, """for (_ <<- @0 & _ <<- @1) { @2!(0) }""")
        _  <- evaluate[IO](runtime, """for (_ <<- @0 & _ <<- @1) { @2!(0) }""")
        _  <- evaluate[IO](runtime, """@1!!(1)""")
        _  <- evaluate[IO](runtime, """@0!(0)""")
        r1 <- runtime.getData(GInt(0L)).map(_.size)
        r2 <- runtime.getData(GInt(1L)).map(_.size)
        r3 <- runtime.getData(GInt(2L)).map(_.size)
        _  = r1 shouldBe 1
        _  = r2 shouldBe 1
      } yield (r3 shouldBe 2)
    }.unsafeRunSync
  }
}
