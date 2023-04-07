package coop.rchain.node.revvaultexport

import cats.effect.unsafe.implicits.global
import com.google.protobuf.ByteString
import coop.rchain.casper.helper.TestNode
import coop.rchain.casper.rholang.BlockRandomSeed
import coop.rchain.models.syntax._
import coop.rchain.casper.util.GenesisBuilder.{buildGenesis, buildGenesisParameters}
import coop.rchain.node.revvaultexport.mainnet1.StateBalanceMain
import coop.rchain.rholang.interpreter.util.RevAddress
import org.scalatest.flatspec.AnyFlatSpec

class VaultBalanceGetterTest extends AnyFlatSpec {
  val genesisInitialBalance = 9000000L

  // Genesis with initial balance on all vaults
  private val genesisParams = buildGenesisParameters()() match {
    case (validatorKeys, genesisVaults, genesisConf) =>
      val newVaults      = genesisConf.vaults.map(_.copy(initialBalance = genesisInitialBalance))
      val newGenesisConf = genesisConf.copy(vaults = newVaults)
      (validatorKeys, genesisVaults, newGenesisConf)
  }
  private val genesis = buildGenesis(genesisParams)

  "Get balance from VaultPar" should "return balance" in {
    val t = TestNode.standaloneEff(genesis).use { node =>
      val genesisPostStateHash =
        genesis.genesisBlock.postStateHash.toBlake2b256Hash
      val genesisVaultAddr = RevAddress.fromPublicKey(genesis.genesisVaults.toList(0)._2).get
      val getVault =
        s"""new return, rl(`rho:registry:lookup`), RevVaultCh, vaultCh, balanceCh in {
          |  rl!(`rho:rchain:revVault`, *RevVaultCh) |
          |  for (@(_, RevVault) <- RevVaultCh) {
          |    @RevVault!("findOrCreate", "${genesisVaultAddr.address.toBase58}", *vaultCh) |
          |    for (@(true, vault) <- vaultCh) {
          |      return!(vault)
          |    }
          |  }
          |}
          |""".stripMargin

      for {
        vaultPar <- node.runtimeManager
                     .playExploratoryDeploy(getVault, genesis.genesisBlock.postStateHash)
        runtime <- node.runtimeManager.spawnRuntime
        _       <- runtime.reset(genesisPostStateHash)
        balance <- VaultBalanceGetter.getBalanceFromVaultPar(vaultPar(0), runtime)
        // 9000000 is hard coded in genesis block generation
        _ = assert(balance.get == genesisInitialBalance)
      } yield ()
    }
    t.unsafeRunSync
  }

  "Get all vault" should "return all vault balance" in {
    val t = TestNode.standaloneEff(genesis).use { node =>
      val genesisPostStateHash =
        genesis.genesisBlock.postStateHash.toBlake2b256Hash
      for {
        runtime               <- node.runtimeManager.spawnRuntime
        _                     <- runtime.reset(genesisPostStateHash)
        vaultTreeHashMapDepth = StateBalanceMain.genesisVaultMapDepth
        vaultChannel <- StateBalances.getGenesisVaultMapPar(
                         genesis.genesisBlock.shardId,
                         runtime
                       )
        storeToken = BlockRandomSeed.storeTokenUnforgeable(
          genesis.genesisBlock.shardId
        )
        balances <- VaultBalanceGetter.getAllVaultBalance(
                     vaultTreeHashMapDepth,
                     vaultChannel,
                     storeToken,
                     runtime
                   )
        // 9000000 is hard coded in genesis block generation
        balancesMap = balances.toMap
        _ = genesis.genesisVaults
          .map { case (_, pub) => RevAddress.fromPublicKey(pub).get }
          .map(addr => RhoTrieTraverser.keccakParString(addr.address.toBase58).drop(2))
          .map(
            byteAddr =>
              assert(balancesMap.get(ByteString.copyFrom(byteAddr)).get == genesisInitialBalance)
          )
      } yield ()
    }
    t.unsafeRunSync
  }

}
