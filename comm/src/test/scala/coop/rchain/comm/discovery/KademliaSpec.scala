package coop.rchain.comm.discovery

import scala.collection.mutable
import cats.Id
import cats.syntax.all._
import cats.effect.IO
import coop.rchain.catscontrib.effect.implicits._
import coop.rchain.comm._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class KademliaSpec extends AnyFunSpec with Matchers with BeforeAndAfterEach {
  val local = createPeer("00000001")
  val peer0 = createPeer("00000010")
  val peer1 = createPeer("00001000")
  val peer2 = createPeer("00001001")
  val peer3 = createPeer("00001010")
  val peer4 = createPeer("00001100")

  val DISTANCE_4 = Some(4)
  val DISTANCE_6 = Some(6)

  describe("A PeertTable with 1 byte addresses and k = 3") {
    describe("when adding a peer to an empty table") {
      it("should add it to a bucket according to its distance") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        table.distance(peer0) shouldBe DISTANCE_6
        // when
        table.updateLastSeen(peer0)
        // then
        bucketEntriesAt(DISTANCE_6, table) shouldEqual Seq(peer0)
      }

      it("should not ping the peer") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        // when
        table.updateLastSeen(peer0)
        // then
        ping.pingedPeers shouldEqual Seq.empty[PeerNode]
      }
    }

    describe("when adding a peer when that peer already exists but with different IP") {
      it("should replace peer with new entry (the one with new IP)") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        table.updateLastSeen(peer1)
        // when
        val newPeer1 = peer1.copy(endpoint = Endpoint("otherIP", 0, 0))
        table.updateLastSeen(newPeer1)
        // then
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(newPeer1)
      }

      it("should move peer to the end of the bucket (meaning it's been seen lately)") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        table.updateLastSeen(peer2)
        table.updateLastSeen(peer1)
        table.updateLastSeen(peer3)
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer1, peer3)
        // when
        val newPeer1 = peer1.copy(endpoint = Endpoint("otherIP", 0, 0))
        table.updateLastSeen(newPeer1)
        // then
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3, newPeer1)
      }
    }

    describe("when adding a peer to a table, where corresponding bucket is filled but not full") {
      it("should add peer to the end of the bucket (meaning it's been seen lately)") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        table.updateLastSeen(peer2)
        table.updateLastSeen(peer3)
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3)
        // when
        table.updateLastSeen(peer1)
        // then
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3, peer1)
      }

      it("no peers should be pinged") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        table.updateLastSeen(peer2)
        table.updateLastSeen(peer3)
        bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3)
        // when
        table.updateLastSeen(peer1)
        // then
        ping.pingedPeers shouldEqual Seq.empty[PeerNode]
      }
    }

    describe("when adding a peer to a table, where corresponding bucket is full") {
      it("should ping the oldest peer to check if it responds") {
        // given
        implicit val ping: KademliaRPCMock = pingOk
        val table                          = PeerTable[PeerNode, IO](local.key, 3)
        thatBucket4IsFull(table)
        // when
        table.updateLastSeen(peer4)
        // then
        ping.pingedPeers shouldEqual Seq(peer1)
      }

      describe("and oldest peer IS responding to ping") {
        it("should drop the new peer") {
          // given
          implicit val ping: KademliaRPCMock = pingOk
          val table                          = PeerTable[PeerNode, IO](local.key, 3)
          thatBucket4IsFull(table)
          // when
          table.updateLastSeen(peer4)
          // then
          bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3, peer1)
        }
      }
      describe("and oldest peer is NOT responding to ping") {
        it("should add the new peer and drop the oldest one") {
          // given
          implicit val ping: KademliaRPCMock = pingFail
          val table                          = PeerTable[PeerNode, IO](local.key, 3)
          thatBucket4IsFull(table)
          // when
          table.updateLastSeen(peer4)
          // then
          bucketEntriesAt(DISTANCE_4, table) shouldEqual Seq(peer2, peer3, peer4)
        }
      }
    }
  }

  private def thatBucket4IsFull(table: PeerTable[PeerNode, IO]): Unit = {
    table.updateLastSeen(peer1)
    table.updateLastSeen(peer2)
    table.updateLastSeen(peer3)
  }

  private def bucketEntriesAt(
      distance: Option[Int],
      table: PeerTable[PeerNode, IO]
  ): Seq[PeerNode] =
    distance.map(d => table.table(d).map(_.entry)).getOrElse(Seq.empty[PeerNode])

  private val pingOk: KademliaRPCMock   = new KademliaRPCMock(returns = true)
  private val pingFail: KademliaRPCMock = new KademliaRPCMock(returns = false)

  private class KademliaRPCMock(returns: Boolean) extends KademliaRPC[IO] {
    val pingedPeers: mutable.MutableList[PeerNode] = mutable.MutableList.empty[PeerNode]

    def ping(peer: PeerNode): IO[Boolean] = {
      pingedPeers += peer
      returns.pure[IO]
    }
    def lookup(key: Seq[Byte], peer: PeerNode): IO[Seq[PeerNode]] = Seq.empty[PeerNode].pure[IO]
    def receive(
        pingHandler: PeerNode => IO[Unit],
        lookupHandler: (PeerNode, Array[Byte]) => IO[Seq[PeerNode]]
    ): IO[Unit]              = ().pure[IO]
    def shutdown(): IO[Unit] = ().pure[IO]
  }

  private def createPeer(id: String): PeerNode = {
    val bytes = (Integer.parseInt(id, 2) & 0xFF).toByte
    PeerNode(NodeIdentifier(Seq(bytes)), Endpoint(id, 0, 0))
  }
}
