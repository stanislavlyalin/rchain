package coop.rchain.node

import cats.effect.IO
import coop.rchain.casper.api.BlockReportApi
import coop.rchain.casper.helper.TestNode
import coop.rchain.casper.rholang.{BlockRandomSeed, Resources}
import coop.rchain.casper.util.ConstructDeploy
import coop.rchain.casper.util.GenesisBuilder.{buildGenesis, GenesisContext}
import coop.rchain.casper.reporting.{ReportStore, ReportingCasper}
import coop.rchain.crypto.{PrivateKey, PublicKey}
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.node.web.{PreCharge, Refund, Transaction, UserDeploy}
import coop.rchain.rholang.interpreter.util.RevAddress
import coop.rchain.models.syntax._
import coop.rchain.rspace.syntax.rspaceSyntaxKeyValueStoreManager
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global

class TransactionAPISpec extends AnyFlatSpec with Matchers with Inspectors {
  val genesis: GenesisContext = buildGenesis()

  def checkTransactionAPI(term: String, phloLimit: Long, phloPrice: Long, deployKey: PrivateKey) =
    TestNode.networkEff(genesis, networkSize = 1, withReadOnlySize = 1).use { nodes =>
      val validator = nodes(0)
      val readonly  = nodes(1)

      import readonly._
      for {
        kvm         <- Resources.mkTestRNodeStoreManager[IO](readonly.dataDir)
        rspaceStore <- kvm.rSpaceStores
        reportingCasper = ReportingCasper
          .rhoReporter[IO](rspaceStore, this.genesis.genesisBlock.shardId)
        reportingStore <- ReportStore.store[IO](kvm)
        blockReportAPI = BlockReportApi[IO](
          reportingCasper,
          reportingStore,
          readonly.validatorIdOpt
        )
        deploy <- ConstructDeploy.sourceDeployNowF[IO](
                   term,
                   sec = deployKey,
                   phloLimit = phloLimit,
                   phloPrice = phloPrice,
                   shardId = this.genesis.genesisBlock.shardId
                 )
        transactionAPI = Transaction[IO](
          blockReportAPI,
          BlockRandomSeed.transferUnforgeable(
            this.genesis.genesisBlock.shardId
          )
        )
        transferBlock <- validator.addBlock(deploy)
        _             <- readonly.addBlock(transferBlock)
        transactions <- transactionAPI
                         .getTransaction(transferBlock.blockHash.toBlake2b256Hash)

      } yield (transactions, transferBlock)
    }

  "transfer rev" should "be gotten in transaction api" in {
    val fromSk      = genesis.genesisVaultsSks.head
    val fromAddr    = RevAddress.fromPublicKey(Secp256k1.toPublic(fromSk)).get.toBase58
    val toPk        = genesis.genesisVaultsSks.last
    val toAddr      = RevAddress.fromPublicKey(Secp256k1.toPublic(toPk)).get.toBase58
    val amount      = 1L
    val phloPrice   = 1L
    val phloLimit   = 3000000L
    val transferRho = s"""
         #new rl(`rho:registry:lookup`), RevVaultCh, vaultCh, toVaultCh, deployerId(`rho:rchain:deployerId`), revVaultKeyCh, resultCh in {
         #  rl!(`rho:rchain:revVault`, *RevVaultCh) |
         #  for (@(_, RevVault) <- RevVaultCh) {
         #    @RevVault!("findOrCreate", "$fromAddr", *vaultCh) |
         #    @RevVault!("findOrCreate", "$toAddr", *toVaultCh) |
         #    @RevVault!("deployerAuthKey", *deployerId, *revVaultKeyCh) |
         #    for (@(true, vault) <- vaultCh; key <- revVaultKeyCh; @(true, toVault) <- toVaultCh) {
         #      @vault!("transfer", "$toAddr", $amount, *key, *resultCh) |
         #      for (_ <- resultCh) { Nil }
         #    }
         #  }
         #}""".stripMargin('#')
    (for {
      result                        <- checkTransactionAPI(transferRho, phloLimit, phloPrice, fromSk)
      (transactions, transferBlock) = result
      _                             = transactions.length should be(3)
      _ = transactions.foreach { t =>
        t.transactionType match {
          case UserDeploy(_) =>
            t.transaction.fromAddr should be(fromAddr)
            t.transaction.toAddr should be(toAddr)
            t.transaction.amount should be(amount)
            t.transaction.failReason should be(None)

          case PreCharge(_) =>
            t.transaction.fromAddr should be(fromAddr)
            t.transaction.amount should be(phloLimit * phloPrice)
            t.transaction.failReason should be(None)

          case Refund(_) =>
            t.transaction.toAddr should be(fromAddr)
            t.transaction.amount should be(
              phloLimit * phloPrice - transferBlock.state.deploys.head.cost.cost
            )
            t.transaction.failReason should be(None)
          case _ => ()
        }
      }
    } yield ()).unsafeRunSync
  }

  "no user deploy log" should "return only precharge and refund transaction" in {
    val fromSk    = genesis.genesisVaultsSks.head
    val fromAddr  = RevAddress.fromPublicKey(Secp256k1.toPublic(fromSk)).get.toBase58
    val phloPrice = 1L
    val phloLimit = 3000000L
    val deployRho = s"""new a in {}"""
    (for {
      result                <- checkTransactionAPI(deployRho, phloLimit, phloPrice, fromSk)
      (transactions, block) = result
      _                     = transactions.length should be(2)
      _ = transactions.foreach { t =>
        t.transactionType match {
          case PreCharge(_) =>
            t.transaction.fromAddr should be(fromAddr)
            t.transaction.amount should be(phloLimit * phloPrice)
            t.transaction.failReason should be(None)

          case Refund(_) =>
            t.transaction.toAddr should be(fromAddr)
            t.transaction.amount should be(
              phloLimit * phloPrice - block.state.deploys.head.cost.cost
            )
            t.transaction.failReason should be(None)
          case _ => ()
        }
      }

    } yield ()).unsafeRunSync
  }

  "preCharge failed case" should "return 1 preCharge transaction" in {
    val fromSk    = genesis.genesisVaultsSks.head
    val fromAddr  = RevAddress.fromPublicKey(Secp256k1.toPublic(fromSk)).get.toBase58
    val phloPrice = 1L
    val phloLimit = 300000000000L
    val deployRho = s"""new a in {}"""
    val (transaction, block) = (for {
      result                <- checkTransactionAPI(deployRho, phloLimit, phloPrice, fromSk)
      (transactions, block) = result
      _                     = transactions.length should be(1)
      t                     = transactions.head

      _ = t.transaction.failReason should be(Some("Insufficient funds"))

    } yield (t, block)).unsafeRunSync
    transaction.transactionType shouldBe a[PreCharge]
    transaction.transaction.fromAddr shouldBe fromAddr
    transaction.transaction.amount shouldBe phloLimit * phloPrice - block.state.deploys.head.cost.cost
    transaction.transaction.failReason shouldBe Some("Insufficient funds")
  }
}
