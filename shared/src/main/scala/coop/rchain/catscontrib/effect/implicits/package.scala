package coop.rchain.catscontrib.effect

import cats._
import cats.arrow.FunctionK
import cats.effect._
import cats.effect.kernel.CancelScope
import cats.syntax.all._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

package object implicits {
  // Sync typeclass implementation for cats.Eval datatype is required to use cats Eval for stack safe serialization of
  // Rholang types. This replaces (as part of attempt to abstract from concrete effect type)
  // monix.Сoeval that was used for this purpose before.
  implicit val sEval = new Sync[Eval] {
    override def suspend[A](hint: Sync.Type)(thunk: => A): Eval[A] = Eval.later(thunk)

    override def rootCancelScope: CancelScope = CancelScope.Cancelable

    override def forceR[A, B](fa: Eval[A])(fb: Eval[B]): Eval[B] = fa.flatMap(_ => fb)

    override def uncancelable[A](body: Poll[Eval] => Eval[A]): Eval[A] = {
      val poll: Poll[Eval] = FunctionK.id[Eval].asInstanceOf[Poll[Eval]]
      body(poll)
    }

    override def canceled: Eval[Unit] = Eval.Unit

    override def onCancel[A](fa: Eval[A], fin: Eval[Unit]): Eval[A] = fin.map(_.asInstanceOf[A])

    override def flatMap[A, B](fa: Eval[A])(f: A => Eval[B]): Eval[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: A => Eval[Either[A, B]]): Eval[B] = a.tailRecM(f)

    override def pure[A](x: A): Eval[A] = Eval.now(x)

    override def monotonic: Eval[FiniteDuration] =
      Eval.always(FiniteDuration(java.lang.System.nanoTime, MILLISECONDS))

    override def realTime: Eval[FiniteDuration] =
      Eval.always(FiniteDuration(java.lang.System.currentTimeMillis, MILLISECONDS))

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    override def raiseError[A](e: Throwable): Eval[A] = Eval.later(throw e)

    override def handleErrorWith[A](fa: Eval[A])(f: Throwable => Eval[A]): Eval[A] =
      try {
        Eval.now(fa.value)
      } catch {
        case NonFatal(e) => f(e)
      }
  }
}
