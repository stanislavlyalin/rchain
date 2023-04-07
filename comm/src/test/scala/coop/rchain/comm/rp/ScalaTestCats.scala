package coop.rchain.comm.rp

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.enablers.Containing

object ScalaTestCats {
  implicit def idContaining[C](implicit C: Containing[C]): Containing[IO[C]] =
    new Containing[IO[C]] {
      def contains(container: IO[C], element: Any): Boolean = {
        val con: C = container.unsafeRunSync()
        C.contains(con, element)
      }
      def containsNoneOf(container: IO[C], elements: Seq[Any]): Boolean = {
        val con: C = container.unsafeRunSync()
        C.containsNoneOf(con, elements)
      }
      def containsOneOf(container: IO[C], elements: Seq[Any]): Boolean = {
        val con: C = container.unsafeRunSync()
        C.containsOneOf(con, elements)
      }
    }
}
