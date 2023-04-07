package coop.rchain.comm.discovery

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import scala.util.Random
import cats.{Id, catsInstancesForId => _}
import coop.rchain.catscontrib.effect.implicits._
import coop.rchain.comm._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PeerTableSpec extends AnyFlatSpec with Matchers with Inside {
  val addressWidth = 8
  val endpoint     = Endpoint("", 0, 0)
  val home         = PeerNode(NodeIdentifier(randBytes(addressWidth)), endpoint)

  private def randBytes(nbytes: Int): Array[Byte] = {
    val arr = Array.fill(nbytes)(0.toByte)
    Random.nextBytes(arr)
    arr
  }

  implicit val ping: KademliaRPC[IO] = new KademliaRPC[IO] {
    def ping(node: PeerNode): IO[Boolean]                         = true.pure[IO]
    def lookup(key: Seq[Byte], peer: PeerNode): IO[Seq[PeerNode]] = Seq.empty[PeerNode].pure[IO]
  }

  "Peer that is already in the table" should "get updated" in {
    val id    = randBytes(addressWidth)
    val peer0 = PeerNode(NodeIdentifier(id), Endpoint("new", 0, 0))
    val peer1 = PeerNode(NodeIdentifier(id), Endpoint("changed", 0, 0))
    val table = PeerTable[PeerNode, IO](home.key)
    table.updateLastSeen(peer0)
    inside(table.peers.unsafeRunSync()) {
      case p +: Nil => p should equal(peer0)
    }
    table.updateLastSeen(peer1)
    inside(table.peers.unsafeRunSync()) {
      case p +: Nil => p should equal(peer1)
    }
  }
}
