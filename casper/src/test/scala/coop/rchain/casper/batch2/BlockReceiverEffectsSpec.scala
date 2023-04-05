package coop.rchain.casper.batch2

import cats.Applicative
import cats.effect.{Concurrent, IO, Sync}
import cats.syntax.all._
import coop.rchain.blockstorage.BlockStore.BlockStore
import coop.rchain.blockstorage.dag.{BlockDagStorage, DagMessageState, DagRepresentation}
import coop.rchain.casper.ValidatorIdentity
import coop.rchain.casper.blocks.BlockRetriever.{AdmitHashResult, Ignore}
import coop.rchain.casper.blocks.{BlockReceiver, BlockReceiverState, BlockRetriever}
import coop.rchain.casper.protocol.{BlockMessage, BlockMessageProto}
import coop.rchain.casper.util.scalatest.Fs2StreamMatchers
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.models.BlockHash.BlockHash
import coop.rchain.models.syntax._
import coop.rchain.shared.Log
import fs2.concurrent.Queue
import org.mockito.captor.ArgCaptor
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.testing.scalatest.AsyncIOSpec
import coop.rchain.shared.RChainScheduler._

import scala.collection.immutable.SortedMap

// TODO enable when CE is migrated to 3 (cats.effect.testing.scalatest is not available for CE2)
//class BlockReceiverEffectsSpec
//    extends AsyncFlatSpec
//    with AsyncIOSpec
//    with Matchers
//    with Fs2StreamMatchers
//    with IdiomaticMockito
//    with IdiomaticMockitoCats
//    with ArgumentMatchersSugar {
//  implicit val logEff: Log[IO] = Log.log[IO]
//
//  it should "pass correct block to output stream with calling effectful components" in
//    withEnv[IO]("root") {
//      case (incomingQueue, _, outStream, bs, br, bds) =>
//        for {
//          block   <- IO.delay(makeBlock())
//          _       <- incomingQueue.enqueue1(block)
//          outList <- outStream.take(1).compile.toList
//        } yield {
//          bs.put(Seq((block.blockHash, block))) wasCalled once
//          bs.contains(Seq(block.blockHash)) wasCalled once
//          br.ackReceived(block.blockHash) wasCalled once
//          dagStorageWasNotModified(bds)
//          outList shouldBe List(block.blockHash)
//        }
//    }
//
//  // Provided to BlockReceiver shard name ("test") is differ from block's shard name ("root" by default)
//  // So block should be rejected and output stream should never take block
//  it should "discard block with invalid shard name" in withEnv[IO]("test") {
//    case (incomingQueue, _, outStream, bs, br, bds) =>
//      for {
//        block <- IO.delay(makeBlock())
//        _     <- incomingQueue.enqueue1(block)
//      } yield {
//        bs.put(*) wasNever called
//        bs.contains(*) wasNever called
//        br.ackReceived(*) wasNever called
//        dagStorageWasNotModified(bds)
//        outStream should notEmit
//      }
//  }
//
//  it should "discard block with invalid block hash" in withEnv[IO]("root") {
//    case (incomingQueue, _, outStream, bs, br, bds) =>
//      for {
//        block <- IO.delay(makeBlock().copy(blockHash = "abc".unsafeHexToByteString))
//        _     <- incomingQueue.enqueue1(block)
//      } yield {
//        bs.put(*) wasNever called
//        bs.contains(*) wasNever called
//        br.ackReceived(*) wasNever called
//        dagStorageWasNotModified(bds)
//        outStream should notEmit
//      }
//  }
//
//  it should "discard block with invalid signature" in withEnv[IO]("root") {
//    case (incomingQueue, _, outStream, bs, br, bds) =>
//      for {
//        block <- IO.delay(makeBlock().copy(sig = "abc".unsafeHexToByteString))
//        _     <- incomingQueue.enqueue1(block)
//      } yield {
//        bs.put(*) wasNever called
//        bs.contains(*) wasNever called
//        br.ackReceived(*) wasNever called
//        dagStorageWasNotModified(bds)
//        outStream should notEmit
//      }
//  }
//
//  it should "pass to output blocks with resolved dependencies" in withEnv[IO]("root") {
//    case (incomingQueue, validatedQueue, outStream, bs, br, bds) =>
//      for {
//        // Received a parent with an empty list of justifications and its child
//        a1 <- IO.delay(makeBlock())
//        a2 = makeBlock(List(a1.blockHash))
//
//        // Put the parent and child in the input queue
//        _ <- incomingQueue.enqueue1(a2)
//        _ <- incomingQueue.enqueue1(a1)
//
//        // Dependencies of the child (its parent) have not yet been resolved,
//        // so only the parent goes to the output queue, since it has no dependencies
//        a1InOutQueue <- outStream.take(1).compile.lastOrError
//
//        // A1 is now validated (e.g. in BlockProcessor)
//        _ <- validatedQueue.enqueue1(a1)
//
//        // All dependencies of child A2 are resolved, so it also goes to the output queue
//        a2InOutQueue <- outStream.take(1).compile.lastOrError
//      } yield {
//        bs.put(Seq((a1.blockHash, a1))) wasCalled once
//        bs.put(Seq((a2.blockHash, a2))) wasCalled once
//
//        val bsContainsCaptor = ArgCaptor[Seq[BlockHash]]
//        bs.contains(bsContainsCaptor) wasCalled 4.times
//        bsContainsCaptor.values should contain allOf (Seq(a1.blockHash), Seq(a2.blockHash))
//
//        br.ackReceived(a1.blockHash) wasCalled once
//        br.ackReceived(a2.blockHash) wasCalled once
//
//        dagStorageWasNotModified(bds)
//        a1InOutQueue shouldBe a1.blockHash
//        a2InOutQueue shouldBe a2.blockHash
//      }
//  }
//
//  private def blockDagStorageMock[F[_]: Applicative]: BlockDagStorage[F] = {
//    val emptyDag = DagRepresentation(Set(), Map(), SortedMap(), DagMessageState(), Map())
//    mock[BlockDagStorage[F]].getRepresentation returnsF emptyDag
//  }
//
//  private def blockRetrieverMock[F[_]: Applicative]: BlockRetriever[F] = {
//    val brMock = mock[BlockRetriever[F]]
//    brMock.ackReceived(*) returns ().pure[F]
//    brMock.admitHash(*, *, *) returnsF AdmitHashResult(
//      Ignore,
//      broadcastRequest = false,
//      requestBlock = false
//    )
//    brMock
//  }
//
//  private def blockStoreMock[F[_]: Sync]: BlockStore[F] = {
//    val state  = Ref.unsafe[F, Map[BlockHash, BlockMessage]](Map())
//    val bsMock = mock[BlockStore[F]]
//    bsMock.contains(*) answers { keys: Seq[BlockHash] =>
//      state.get.map(s => Seq(s.contains(keys.head)))
//    }
//    bsMock.put(*) answers { kvPairs: Seq[(BlockHash, BlockMessage)] =>
//      state.update(s => kvPairs.foldLeft(s) { case (acc, item) => acc + item })
//    }
//    bsMock
//  }
//
//  import fs2._
//
//  private def withEnv[F[_]: Concurrent: Log](shardId: String)(
//      f: (
//          Queue[F, BlockMessage],
//          Queue[F, BlockMessage],
//          Stream[F, BlockHash],
//          BlockStore[F],
//          BlockRetriever[F],
//          BlockDagStorage[F]
//      ) => F[Assertion]
//  ): F[Assertion] =
//    for {
//      state                 <- Ref[F].of(BlockReceiverState[BlockHash])
//      incomingBlockQueue    <- Queue.unbounded[F, BlockMessage]
//      incomingBlockStream   = incomingBlockQueue.dequeue
//      validatedBlocksQueue  <- Queue.unbounded[F, BlockMessage]
//      validatedBlocksStream = validatedBlocksQueue.dequeue
//
//      // Create mock separately for each test
//      bs  = blockStoreMock[F]
//      br  = blockRetrieverMock[F]
//      bds = blockDagStorageMock[F]
//
//      blockReceiver <- {
//        implicit val (bsImp, brImp, bdsImp) = (bs, br, bds)
//        BlockReceiver(
//          state,
//          incomingBlockStream,
//          validatedBlocksStream,
//          shardId,
//          incomingBlockQueue.enqueue1
//        )
//      }
//      res <- f(incomingBlockQueue, validatedBlocksQueue, blockReceiver, bs, br, bds)
//    } yield res
//
//  private def makeDefaultBlock =
//    BlockMessage
//      .from(
//        BlockMessageProto(
//          shardId = "root",
//          postStateHash = "abc".unsafeHexToByteString,
//          sigAlgorithm = Secp256k1.name
//        )
//      )
//      .right
//      .get
//
//  private def makeBlock(justifications: List[BlockHash] = List()): BlockMessage = {
//    val (privateKey, pubKey) = Secp256k1.newKeyPair
//    val block =
//      makeDefaultBlock.copy(sender = pubKey.bytes.toByteString, justifications = justifications)
//    ValidatorIdentity(privateKey).signBlock(block)
//  }
//
//  private def dagStorageWasNotModified[F[_]](bds: BlockDagStorage[F]) = {
//    bds.insert(*, *) wasNever called
//    bds.addDeploy(*) wasNever called
//  }
//}
