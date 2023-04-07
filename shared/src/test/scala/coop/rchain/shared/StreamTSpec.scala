package coop.rchain.shared

import cats._
import cats.effect.IO
import cats.syntax.all._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.{Failure, Success, Try}

class StreamTSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import StreamTSpec._

  describe("StreamT") {
    it("should be able to be constructed from lists") {
      forAll { (list: List[Int]) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.toList[Int] shouldBe list
      }
    }

    it("should correctly compute heads") {
      forAll { (list: List[Int]) =>
        whenever(list.nonEmpty) {
          val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

          stream.head[Int](mErrId) shouldBe list.head
        }
      }
    }

    it("should correctly compute tails") {
      forAll { (list: List[Int]) =>
        whenever(list.nonEmpty) {
          val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

          stream.tail(mErrId).toList[Int] shouldBe list.tail
        }
      }
    }

    it("should be able to zip with other StreamTs") {
      forAll { (listA: List[Int], listB: List[String]) =>
        val streamA = StreamT.fromList[IO, Int](listA.pure[IO])
        val streamB = StreamT.fromList[IO, String](listB.pure[IO])

        listA.zip(listB) shouldBe streamA.zip(streamB).toList
      }
    }

    it("should allow taking a finite number of terms") {
      forAll { (list: List[Int], n: Int) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.take(n).toList[Int] shouldBe list.take(n)
      }

    }

    it(
      "should allow taking the longest prefix of this StreamT whose elements satisfy the predicate"
    ) {
      forAll { list: List[Int] =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.takeWhile(_ < 100).toList[Int] shouldBe list.takeWhile(_ < 100)
      }

    }

    it("should allow dropping a finite number of terms") {
      forAll { (list: List[Int], n: Int) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.drop(n).toList[Int] shouldBe list.drop(n)
      }

    }

    it("should allow dropping a finite number of terms until a term doesn't satisfy the predicate") {
      forAll { list: List[Int] =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.dropWhile(_ < 100).toList[Int] shouldBe list.dropWhile(_ < 100)
      }
    }

    it("should find elements properly in") {
      forAll { (list: List[Int]) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.find[Int](_ % 2 == 0) shouldBe list.find(_ % 2 == 0)
      }
    }

    it("should foldLeft properly in") {
      forAll { (list: List[Int]) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.foldLeft[Int](0)(_ + _) shouldBe list.sum
      }
    }

    it("should map properly in") {
      forAll { (list: List[Int]) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream.map(_ * 2).toList[Int] shouldBe list.map(_ * 2)
      }
    }

    it("should flatMap properly in") {
      forAll { (list: List[Int]) =>
        val stream: StreamT[IO, Int] = StreamT.fromList[IO, Int](list.pure[IO])

        stream
          .flatMap(i => StreamT.fromList[IO, Int](List(i + 1, i + 2, i + 3).pure[IO]))
          .toList[Int] shouldBe list.flatMap(i => List(i + 1, i + 2, i + 3))
      }
    }
    it("should be able lazily construct infinite sequences") {
      lazy val fibs: StreamT[IO, Long] =
        StreamT.cons(
          0L,
          Eval.now(pure(StreamT.cons(1L, Eval.later(pure(fibs.zip(fibs.tail(mErrId)).map {
            case (a, b) => a + b
          })))))
        )

      fibs.take(8).toList[Long] shouldBe List(0L, 1L, 1L, 2L, 3L, 5L, 8L, 13L)
    }
  }

  private def pure[A](value: A): IO[A] = Applicative[IO].pure(value)
}

object StreamTSpec {
  val mErrId = unsafeMErr[IO]

  def unsafeMErr[F[_]: Monad]: MonadError[F, Throwable] =
    new MonadError[F, Throwable] {
      def pure[A](x: A): F[A]                                 = Monad[F].pure(x)
      def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]         = Monad[F].flatMap(fa)(f)
      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] = Monad[F].tailRecM(a)(f)
      def raiseError[A](e: Throwable): F[A]                   = throw e
      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] = Try(fa) match {
        case Success(x) => x
        case Failure(e) => f(e)
      }
    }
}
