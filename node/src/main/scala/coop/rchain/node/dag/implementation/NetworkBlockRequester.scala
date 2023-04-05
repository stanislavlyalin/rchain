package coop.rchain.node.dag.implementation

import cats.effect.{Async, Sync}
import coop.rchain.sdk.block.BlockRequester
import fs2.Stream
import cats.effect.Ref

object NetworkBlockRequester {
  def apply[F[_]: Async, B, BId](
      st: Ref[F, Map[BId, BlockStatus[B, BId]]]
  ): F[NetworkBlockRequester[F, B, BId]] =
    Sync[F].delay(new NetworkBlockRequester(st))
}

sealed trait BlockStatus[B, BId]
final case class Requested[B, BId](id: BId)      extends BlockStatus[B, BId]
final case class Received[B, BId](id: BId, b: B) extends BlockStatus[B, BId]

/**
  * TODO: Should wrap existing BlockRequester exposing necessary block statuses.
  */
final case class NetworkBlockRequester[F[_]: Async, B, BId] private (
    st: Ref[F, Map[BId, BlockStatus[B, BId]]]
) extends BlockRequester[F, B, BId] {
  override def requestBlock(id: BId): F[Unit] = ???

  override def response: Stream[F, B] = ???
}
