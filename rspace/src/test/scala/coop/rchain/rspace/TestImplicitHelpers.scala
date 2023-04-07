package coop.rchain.rspace
import cats.Id
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.enablers.Definition

//noinspectTaskn ConvertExpressionToSAM
trait TestImplicitHelpers {
  // Some helpers for usage only in the tests -- save us A LOT of explicit casting from Either to Option
  // it is safe because left type of `Either` is `Nothing` -- we don't expect any invalid states from the matcher
  implicit def eitherDefinitionScalatest[E, A]: Definition[IO[Either[E, Option[A]]]] =
    new Definition[IO[Either[E, Option[A]]]] {
      override def isDefined(thing: IO[Either[E, Option[A]]]): Boolean =
        thing.unsafeRunSync().right.get.isDefined
    }
}
