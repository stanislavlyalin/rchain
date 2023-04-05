package coop.rchain.casper.genesis

import cats.syntax.all._
import coop.rchain.casper.genesis.contracts.{ProofOfStake, Registry, Validator, Vault}
import coop.rchain.casper.helper.TestNode
import coop.rchain.casper.helper.TestNode._
import coop.rchain.casper.util.ConstructDeploy
import coop.rchain.crypto.PrivateKey
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.p2p.EffectsTestInstances.LogicalTime
import coop.rchain.shared.scalatestcontrib._
import coop.rchain.models.syntax._
import coop.rchain.rholang.build.CompiledRholangTemplate
import coop.rchain.rholang.interpreter.util.RevAddress
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers

class PosMultiSigTransferSpec extends AnyFlatSpec with Matchers with Inspectors {

  import coop.rchain.casper.util.GenesisBuilder._

  implicit val timeEff = new LogicalTime[Effect]
  val shardId          = "root"
  val p1 =
    PrivateKey("fc743bd08a822d544bfbe05a5663fc325039a44c8f0c7fbea95a85517da5c36b".unsafeDecodeHex)
  val pub1 = Secp256k1.toPublic(p1)
  val p2 =
    PrivateKey("6e88cf274735f3f7f73ec3d7f0362c439ab508427682b5bd788007aca665d810".unsafeDecodeHex)
  val pub2 = Secp256k1.toPublic(p2)
  val p3 =
    PrivateKey("87369d132ed6a7626dc4c5dfbaf41e954dd0ec55830e613a3f868c74d64a7a22".unsafeDecodeHex)
  val pub3          = Secp256k1.toPublic(p3)
  val validatorKeys = Seq((p1, pub1), (p2, pub2), (p3, pub3))

  val pk1 = PrivateKey(
    "1533219a4bea67e7b852101c8fb8e836e5812dd15f931d8d40a99eec9393ef22".unsafeDecodeHex
  )

  val genesisPubKey = Secp256k1.toPublic(pk1)
  val bonds         = Map(pub1 -> 1000000L, pub2 -> 1000000L, pub3 -> 1000000L)

  val configRegistryPubKey =
    "04e2eb6b06058d10b30856043c29076e2d2d7c374d2beedded6ecb8d1df585dfa583bd7949085ac6b0761497b0cfd056eb3d0db97efb3940b14c00fff4e53c85bf"

  val genesisParam: GenesisParameters =
    (
      validatorKeys,
      validatorKeys,
      Genesis(
        sender = pub1, // First key as genesis creator
        shardId = shardId,
        proofOfStake = ProofOfStake(
          minimumBond = 1L,
          maximumBond = Long.MaxValue,
          epochLength = 1000,
          quarantineLength = 50000,
          numberOfActiveValidators = 100,
          validators = bonds.map(Validator.tupled).toSeq,
          posMultiSigPublicKeys = Seq(pub1, pub2, pub3).map(p => p.bytes.toHexString).toList,
          posVaultPubKey = genesisPubKey.bytes.toHexString,
          posMultiSigQuorum = 2
        ),
        registry = Registry(configRegistryPubKey),
        vaults = List((pub1, 100000000000L), (pub2, 100000000000L), (pub3, 100000000000L)).map {
          case (pk, amount) =>
            RevAddress.fromPublicKey(pk).map(Vault(_, amount))
        }.flattenOption,
        blockNumber = 0
      )
    )

  val genesis = buildGenesis(genesisParam)

  "pos multisig vault transfer" should "work with enough quorum confirm" in effectTest {
    val transferAmount = 1000000

    val (_, targetPub) = Secp256k1.newKeyPair
    val targetAddr     = RevAddress.fromPublicKey(targetPub).get.address.toBase58
    val transfer = CompiledRholangTemplate.loadTemplate(
      "MultiSigVault/PosMultiSigTransfer.rho",
      Seq(
        ("targetAddr", targetAddr),
        ("amount", transferAmount)
      )
    )
    val confirm =
      CompiledRholangTemplate.loadTemplate(
        "MultiSigVault/PosMultiSigConfirm.rho",
        Seq(
          ("targetAddr", targetAddr),
          ("amount", transferAmount),
          ("nonce", 0)
        )
      )

    val initialTransferToPosMultiSig = CompiledRholangTemplate.loadTemplate(
      "MultiSigVault/TransferToPosMultiSig.rho",
      Seq(
        ("from", RevAddress.fromPublicKey(pub1).get.address.toBase58),
        ("amount", transferAmount)
      )
    )

    val getBalance = s"""new return, rl(`rho:registry:lookup`), RevVaultCh, vaultCh, balanceCh in {
                       #  rl!(`rho:rchain:revVault`, *RevVaultCh) |
                       #  for (@(_, RevVault) <- RevVaultCh) {
                       #    @RevVault!("findOrCreate", "$targetAddr", *vaultCh) |
                       #    for (@(true, vault) <- vaultCh) {
                       #      @vault!("balance", *balanceCh) |
                       #      for (@balance <- balanceCh) {
                       #        return!(balance)
                       #      }
                       #    }
                       #  }
                       #}""".stripMargin('#')
    // initial transfer to make sure pos multisig vault get some money
    val initialTransfer =
      ConstructDeploy.sourceDeployNow(initialTransferToPosMultiSig, p1, 1000000L, shardId = shardId)

    val transferDeploy = ConstructDeploy.sourceDeployNow(transfer, p1, 1000000L, shardId = shardId)

    val confirmDeploy = ConstructDeploy.sourceDeployNow(confirm, p2, 1000000L, shardId = shardId)

    TestNode.standaloneEff(genesis).use { node =>
      val rm = node.runtimeManager
      for {
        b1   <- node.addBlock(initialTransfer)
        _    = assert(b1.state.deploys.head.systemDeployError.isEmpty)
        _    = assert(!b1.state.deploys.head.isFailed)
        b2   <- node.addBlock(transferDeploy)
        _    = assert(b2.state.deploys.head.systemDeployError.isEmpty)
        _    = assert(!b2.state.deploys.head.isFailed)
        b3   <- node.addBlock(confirmDeploy)
        _    = assert(b3.state.deploys.head.systemDeployError.isEmpty)
        _    = assert(!b3.state.deploys.head.isFailed)
        ret2 <- rm.playExploratoryDeploy(getBalance, b3.postStateHash)
        _    = assert(ret2.head.exprs.head.getGInt == transferAmount)
      } yield ()
    }
  }

}
