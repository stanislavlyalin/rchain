package coop.rchain.casper.protocol.client

import cats.effect.{AsyncEffect, Sync}
import cats.syntax.all._
import coop.rchain.casper.protocol._
import coop.rchain.casper.protocol.deploy.v1.{DeployExecStatus, DeployServiceFs2Grpc}
import coop.rchain.crypto.signatures.Signed
import coop.rchain.models.Par
import coop.rchain.shared.syntax._
import coop.rchain.models.either.implicits._
import io.grpc.netty.NettyChannelBuilder
import io.grpc.{ManagedChannel, Metadata}

import java.io.Closeable
import java.util.concurrent.TimeUnit

trait DeployService[F[_]] {
  def deploy(d: Signed[DeployData]): F[Either[Seq[String], String]]
  def deployStatus(deployId: FindDeployQuery): F[Either[Seq[String], DeployExecStatus]]
  def getBlock(q: BlockQuery): F[Either[Seq[String], String]]
  def getBlocks(q: BlocksQuery): F[Either[Seq[String], String]]
  def visualizeDag(q: VisualizeDagQuery): F[Either[Seq[String], String]]
  def machineVerifiableDag(q: MachineVerifyQuery): F[Either[Seq[String], String]]
  def findDeploy(request: FindDeployQuery): F[Either[Seq[String], String]]
  def listenForDataAtName(request: DataAtNameQuery): F[Either[Seq[String], Seq[DataWithBlockInfo]]]
  def listenForContinuationAtName(
      request: ContinuationAtNameQuery
  ): F[Either[Seq[String], Seq[ContinuationsWithBlockInfo]]]
  def getDataAtPar(
      request: DataAtNameByBlockQuery
  ): F[Either[Seq[String], (Seq[Par], LightBlockInfo)]]
  def lastFinalizedBlock: F[Either[Seq[String], String]]
  def isFinalized(q: IsFinalizedQuery): F[Either[Seq[String], String]]
  def bondStatus(q: BondStatusQuery): F[Either[Seq[String], String]]
  def status: F[Either[Seq[String], String]]
}

object DeployService {
  def apply[F[_]](implicit ev: DeployService[F]): DeployService[F] = ev
}

class GrpcDeployService[F[_]: Sync: AsyncEffect](host: String, port: Int, maxMessageSize: Int)
    extends DeployService[F]
    with Closeable {

  private val channel: ManagedChannel =
    NettyChannelBuilder
      .forAddress(host, port)
      .maxInboundMessageSize(maxMessageSize)
      .usePlaintext()
      .build

  private val stub = DeployServiceFs2Grpc.stub(channel)

  def deploy(d: Signed[DeployData]): F[Either[Seq[String], String]] =
    stub
      .doDeploy(DeployData.toProto(d), new Metadata)
      .toEitherF(
        _.message.error,
        _.message.result
      )

  def deployStatus(deployId: FindDeployQuery): F[Either[Seq[String], DeployExecStatus]] =
    stub
      .deployStatus(deployId, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.deployExecStatus
      )

  def getBlock(q: BlockQuery): F[Either[Seq[String], String]] =
    stub
      .getBlock(q, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.blockInfo.map(_.toProtoString)
      )

  def findDeploy(q: FindDeployQuery): F[Either[Seq[String], String]] =
    stub
      .findDeploy(q, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.blockInfo.map(_.toProtoString)
      )

  def visualizeDag(q: VisualizeDagQuery): F[Either[Seq[String], String]] =
    stub
      .visualizeDag(q, new Metadata)
      .evalMap(_.pure[F].toEitherF(_.message.error, _.message.content))
      .compile
      .toList
      .map { bs =>
        val (l, r) = bs.partition(_.isLeft)
        if (l.isEmpty) Right(r.map(_.right.get).mkString)
        else Left(l.flatMap(_.left.get))
      }

  def machineVerifiableDag(q: MachineVerifyQuery): F[Either[Seq[String], String]] =
    stub
      .machineVerifiableDag(q, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.content
      )

  def getBlocks(q: BlocksQuery): F[Either[Seq[String], String]] =
    stub
      .getBlocks(q, new Metadata)
      .evalMap(_.pure[F].toEitherF(_.message.error, _.message.blockInfo))
      .map(_.map { bi =>
        s"""
         |------------- block ${bi.blockNumber} ---------------
         |${bi.toProtoString}
         |-----------------------------------------------------
         |""".stripMargin
      })
      .compile
      .toList
      .map { bs =>
        val (l, r) = bs.partition(_.isLeft)
        if (l.isEmpty) {
          val showLength =
            s"""
             |count: ${r.length}
             |""".stripMargin

          Right(r.map(_.right.get).mkString("\n") + "\n" + showLength)
        } else Left(l.flatMap(_.left.get))
      }

  def listenForDataAtName(
      request: DataAtNameQuery
  ): F[Either[Seq[String], Seq[DataWithBlockInfo]]] =
    stub
      .listenForDataAtName(request, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.payload.map(_.blockInfo)
      )

  def listenForContinuationAtName(
      request: ContinuationAtNameQuery
  ): F[Either[Seq[String], Seq[ContinuationsWithBlockInfo]]] =
    stub
      .listenForContinuationAtName(request, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.payload.map(_.blockResults)
      )

  def getDataAtPar(
      request: DataAtNameByBlockQuery
  ): F[Either[Seq[String], (Seq[Par], LightBlockInfo)]] =
    stub
      .getDataAtName(request, new Metadata)
      .toEitherF(
        _.message.error,
        _.message.payload.map(r => (r.par, r.block))
      )

  def lastFinalizedBlock: F[Either[Seq[String], String]] =
    stub
      .lastFinalizedBlock(LastFinalizedBlockQuery(), new Metadata)
      .toEitherF(
        _.message.error,
        _.message.blockInfo.map(_.toProtoString)
      )

  def isFinalized(request: IsFinalizedQuery): F[Either[Seq[String], String]] =
    stub
      .isFinalized(request, new Metadata)
      .toEitherF(_.message.error, _.message.isFinalized)
      .map(
        _.ifM(
          "Block is finalized".asRight,
          Seq("Block is not finalized").asLeft
        )
      )

  def bondStatus(request: BondStatusQuery): F[Either[Seq[String], String]] =
    stub
      .bondStatus(request, new Metadata)
      .toEitherF(_.message.error, _.message.isBonded)
      .map(
        _.ifM(
          "Validator is bonded".asRight,
          Seq("Validator is not bonded").asLeft
        )
      )

  def status: F[Either[Seq[String], String]] =
    stub
      .status(com.google.protobuf.empty.Empty(), new Metadata)
      .toEitherF(
        _.message.error,
        _.message.status.map(_.toProtoString)
      )

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  override def close(): Unit = {
    val terminated = channel.shutdown().awaitTermination(10, TimeUnit.SECONDS)
    if (!terminated) {
      println(
        "warn: did not shutdown after 10 seconds, retrying with additional 10 seconds timeout"
      )
      channel.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
