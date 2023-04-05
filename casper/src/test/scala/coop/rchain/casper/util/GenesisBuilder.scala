package coop.rchain.casper.util

import cats.effect.IO
import cats.syntax.all._
import coop.rchain.blockstorage.BlockStore
import coop.rchain.blockstorage.syntax._
import coop.rchain.casper.ValidatorIdentity
import coop.rchain.casper.dag.BlockDagKeyValueStorage
import coop.rchain.casper.genesis.Genesis
import coop.rchain.casper.genesis.contracts._
import coop.rchain.casper.protocol._
import coop.rchain.casper.rholang.Resources.mkTestRNodeStoreManager
import coop.rchain.casper.rholang.{BlockRandomSeed, RuntimeManager}
import coop.rchain.casper.util.ConstructDeploy._
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.crypto.{PrivateKey, PublicKey}
import coop.rchain.metrics
import coop.rchain.metrics.{Metrics, NoopSpan}
import coop.rchain.rholang.interpreter.util.RevAddress
import coop.rchain.rspace.syntax._
import coop.rchain.shared.Log
import coop.rchain.shared.syntax._

import java.nio.file.{Files, Path}
import scala.collection.compat.immutable.LazyList
import scala.collection.mutable

object GenesisBuilder {

  val fixedValidatorKeyPairs = List((defaultSec, defaultPub), (defaultSec2, defaultPub2))

  val randomValidatorKeyPairs                  = LazyList.continually(Secp256k1.newKeyPair)
  val (randomValidatorSks, randomValidatorPks) = randomValidatorKeyPairs.unzip

  val defaultPosMultiSigPublicKeys = List(
    "04db91a53a2b72fcdcb201031772da86edad1e4979eb6742928d27731b1771e0bc40c9e9c9fa6554bdec041a87cee423d6f2e09e9dfb408b78e85a4aa611aad20c",
    "042a736b30fffcc7d5a58bb9416f7e46180818c82b15542d0a7819d1a437aa7f4b6940c50db73a67bfc5f5ec5b5fa555d24ef8339b03edaa09c096de4ded6eae14",
    "047f0f0f5bbe1d6d1a8dac4d88a3957851940f39a57cd89d55fe25b536ab67e6d76fd3f365c83e5bfe11fe7117e549b1ae3dd39bfc867d1c725a4177692c4e7754"
  )

  val defaultPosVaultPubKey =
    "0432946f7f91f8f767d7c3d43674faf83586dffbd1b8f9278a5c72820dc20308836299f47575ff27f4a736b72e63d91c3cd853641861f64e08ee5f9204fc708df6"

  def createBonds(validators: Iterable[PublicKey]): Iterable[(PublicKey, Long)] =
    validators.zipWithIndex.map { case (v, i) => v -> (2L * i.toLong + 1L) }

  def createGenesis(): BlockMessage =
    buildGenesis().genesisBlock

  /*
   * buildGenesisParameters and buildGenesis functions have very strange combinations with TestNode
   *  to create network based on number of validators (and read-only nodes).
   * TestNode network creation function accepts number of nodes to create together with result from
   *  buildGenesis function which also accepts genesis parameters with specific number of nodes.
   */

  def buildGenesisParametersSize(numOfValidators: Int): GenesisParameters = {
    val validators = randomValidatorKeyPairs.take(numOfValidators).toList
    buildGenesisParameters(validators)()
  }

  def buildGenesisParametersFromBonds(bonds: List[Long]): GenesisParameters = {
    val validators = randomValidatorKeyPairs.take(bonds.size).toList
    val bondsPair  = validators.map(_._2).zip(bonds)
    buildGenesisParameters(validators)(bondsPair)
  }
  val defaultSystemContractPubKey =
    "04e2eb6b06058d10b30856043c29076e2d2d7c374d2beedded6ecb8d1df585dfa583bd7949085ac6b0761497b0cfd056eb3d0db97efb3940b14c00fff4e53c85bf"

  def buildGenesisParameters(
      validatorKeyPairs: Seq[(PrivateKey, PublicKey)],
      genesisVaults: Seq[(PrivateKey, Long)],
      bonds: Map[PublicKey, Long]
  ): GenesisParameters =
    buildGenesisParameters(validatorKeyPairs.toList)(bonds) match {
      case (validatorKeys, _, genesisConf) =>
        // Use default build parameters function and modify vaults
        val newVaults = genesisVaults.toList.map {
          case (p, s) => Vault(RevAddress.fromPublicKey(Secp256k1.toPublic(p)).get, s)
        }
        val newGenesisVaults = genesisVaults.map { case (k, _) => (k, Secp256k1.toPublic(k)) }
        val newGenesisConf   = genesisConf.copy(vaults = newVaults)
        (validatorKeys, newGenesisVaults, newGenesisConf)
    }

  def buildGenesisParameters(
      validatorKeyPairs: List[(PrivateKey, PublicKey)] = randomValidatorKeyPairs.take(4).toList
  )(
      bonds: Iterable[(PublicKey, Long)] = createBonds(validatorKeyPairs.map(_._2))
  ): GenesisParameters = {
    val (_, firstValidatorPubKey) = validatorKeyPairs.head
    // Genesis vaults includes fixed keys to be always accessible for deploys
    val genesisVaults = fixedValidatorKeyPairs ++ validatorKeyPairs
    (
      validatorKeyPairs,
      genesisVaults,
      Genesis(
        sender = firstValidatorPubKey,
        shardId = "root",
        proofOfStake = ProofOfStake(
          minimumBond = 1L,
          maximumBond = Long.MaxValue,
          // Epoch length is set to large number to prevent trigger of epoch change
          // in PoS close block method, which causes block merge conflicts
          // - epoch change can be set as a parameter in Rholang tests (e.g. PosSpec)
          epochLength = 1000,
          quarantineLength = 50000,
          numberOfActiveValidators = 100,
          validators = bonds.map(Validator.tupled).toSeq,
          posMultiSigPublicKeys = defaultPosMultiSigPublicKeys,
          posMultiSigQuorum = defaultPosMultiSigPublicKeys.length - 1,
          posVaultPubKey = defaultPosVaultPubKey
        ),
        registry = Registry(defaultSystemContractPubKey),
        vaults = genesisVaults.toList.map(pair => predefinedVault(pair._2)) ++
          bonds.toList.map {
            case (pk, _) =>
              // Initial validator vaults contain 0 Rev
              RevAddress.fromPublicKey(pk).map(Vault(_, 0))
          }.flattenOption,
        blockNumber = 0
      )
    )
  }

  private def predefinedVault(pub: PublicKey): Vault =
    Vault(RevAddress.fromPublicKey(pub).get, 9000000)

  type GenesisParameters =
    (Iterable[(PrivateKey, PublicKey)], Iterable[(PrivateKey, PublicKey)], Genesis)

  private val genesisCache: mutable.HashMap[GenesisParameters, GenesisContext] =
    mutable.HashMap.empty

  private var cacheAccesses = 0
  private var cacheMisses   = 0

  def buildGenesis(parameters: GenesisParameters = buildGenesisParametersSize(4)): GenesisContext =
    genesisCache.synchronized {
      cacheAccesses += 1
      genesisCache.getOrElseUpdate(parameters, doBuildGenesis(parameters))
    }

  def buildGenesis(validatorsNum: Int): GenesisContext =
    genesisCache.synchronized {
      cacheAccesses += 1
      val parameters = buildGenesisParametersSize(validatorsNum + 5)
      genesisCache.getOrElseUpdate(
        parameters,
        doBuildGenesis(parameters)
      )
    }

  private def doBuildGenesis(
      parameters: GenesisParameters
  ): GenesisContext = {
    cacheMisses += 1
    println(
      f"""Genesis block cache miss, building a new genesis.
         |Cache misses: $cacheMisses / $cacheAccesses (${cacheMisses.toDouble / cacheAccesses}%1.2f) cache accesses.
       """.stripMargin
    )

    val (validavalidatorKeyPairs, genesisVaults, genesisParameters) = parameters
    val storageDirectory                                            = Files.createTempDirectory(s"hash-set-casper-test-genesis-")
    implicit val log: Log.NOPLog[IO]                                = new Log.NOPLog[IO]
    implicit val metricsEff: Metrics[IO]                            = new metrics.Metrics.MetricsNOP[IO]
    implicit val spanEff                                            = NoopSpan[IO]()

    import coop.rchain.shared.RChainScheduler._

    (for {
      kvsManager <- mkTestRNodeStoreManager[IO](storageDirectory)
      rStore     <- kvsManager.rSpaceStores
      mStore     <- RuntimeManager.mergeableStore(kvsManager)
      t          = RuntimeManager.noOpExecutionTracker[IO]
      runtimeManager <- RuntimeManager(
                         rStore,
                         mStore,
                         BlockRandomSeed.nonNegativeMergeableTagName(parameters._3.shardId),
                         t,
                         rholangEC
                       )
      // First bonded validator is the creator
      creator = ValidatorIdentity(parameters._1.head._1)
      genesis <- {
        implicit val rm = runtimeManager
        Genesis.createGenesisBlock[IO](creator, genesisParameters)
      }
      blockStore      <- BlockStore[IO](kvsManager)
      _               <- blockStore.put(genesis.blockHash, genesis)
      blockDagStorage <- BlockDagKeyValueStorage.create[IO](kvsManager)
      // Add genesis block to DAG
      _ <- blockDagStorage.insertGenesis(genesis)
    } yield GenesisContext(genesis, validavalidatorKeyPairs, genesisVaults, storageDirectory)).unsafeRunSync
  }

  case class GenesisContext(
      genesisBlock: BlockMessage,
      validatorKeyPairs: Iterable[(PrivateKey, PublicKey)],
      genesisVaults: Iterable[(PrivateKey, PublicKey)],
      storageDirectory: Path
  ) {
    def validatorSks: Iterable[PrivateKey] = validatorKeyPairs.map(_._1)
    def validatorPks: Iterable[PublicKey]  = validatorKeyPairs.map(_._2)

    def genesisVaultsSks: Iterable[PrivateKey] = genesisVaults.map(_._1)
    def genesisVailtsPks: Iterable[PublicKey]  = genesisVaults.map(_._2)
  }
}
