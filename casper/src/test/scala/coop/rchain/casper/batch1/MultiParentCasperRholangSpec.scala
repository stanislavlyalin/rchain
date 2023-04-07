package coop.rchain.casper.batch1

import coop.rchain.casper.helper.TestNode
import coop.rchain.casper.helper.TestNode.Effect
import coop.rchain.casper.protocol.BlockMessage
import coop.rchain.casper.rholang.{BlockRandomSeed, RuntimeManager, Tools}
import coop.rchain.casper.util.{ConstructDeploy, ProtoUtil, RSpaceUtil}
import coop.rchain.crypto.signatures.Secp256k1
import coop.rchain.p2p.EffectsTestInstances.LogicalTime
import coop.rchain.rholang.interpreter.SystemProcesses.BlockData
import coop.rchain.models.syntax._
import coop.rchain.shared.Base16
import coop.rchain.shared.scalatestcontrib._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers

class MultiParentCasperRholangSpec extends AnyFlatSpec with Matchers with Inspectors {

  import RSpaceUtil._
  import coop.rchain.casper.util.GenesisBuilder._

  implicit val timeEff: LogicalTime[Effect] = new LogicalTime[Effect]

  val genesis = buildGenesis()

  "MultiParentCasper" should "create blocks based on deploys" in effectTest {
    TestNode.standaloneEff(genesis).use { implicit node =>
      implicit val rm: RuntimeManager[Effect] = node.runtimeManager

      for {
        deploy         <- ConstructDeploy.basicDeployData[Effect](0)
        block          <- node.createBlockUnsafe(deploy)
        deploys        = block.state.deploys.map(_.deploy)
        justifications = block.justifications

        _      = justifications.size should be(1)
        _      = justifications.head should be(genesis.genesisBlock.blockHash)
        _      = deploys.size should be(1)
        _      = deploys.head should be(deploy)
        data   <- getDataAtPublicChannel[Effect](block, 0)
        result = data shouldBe Seq("0")
      } yield result
    }
  }

  it should "be able to use the registry" in effectTest {
    TestNode.standaloneEff(genesis).use { node =>
      implicit val rm: RuntimeManager[Effect] = node.runtimeManager

      val registerSource =
        """
          |new uriCh, rr(`rho:registry:insertArbitrary`), hello in {
          |  contract hello(@name, return) = {
          |    return!("Hello, ${name}!" %% {"name" : name})
          |  } |
          |  rr!(bundle+{*hello}, *uriCh)
          |}
        """.stripMargin

      def callSource(registryId: String) =
        s"""
           |new out, rl(`rho:registry:lookup`), helloCh in {
           |  rl!($registryId, *helloCh) |
           |  for(hello <- helloCh){
           |    hello!("World", *out)
           |  }
           |}
         """.stripMargin

      def calculateDeployUnforgeableName(block: BlockMessage): String = {
        val rand              = BlockRandomSeed.randomGenerator(block)
        val deployIndex: Byte = 0
        rand
          .splitByte(deployIndex)
          .splitByte(BlockRandomSeed.UserDeploySplitIndex)
          .next()
          .toHexString
      }

      for {
        registerDeploy <- ConstructDeploy
                           .sourceDeployNowF[Effect](
                             registerSource,
                             shardId = genesis.genesisBlock.shardId
                           )
        block0 <- node.addBlock(registerDeploy)
        registryId <- getDataAtPrivateChannel[Effect](
                       block0,
                       calculateDeployUnforgeableName(block0)
                     )
        callDeploy <- ConstructDeploy.sourceDeployNowF[Effect](
                       callSource(registryId.head),
                       shardId = genesis.genesisBlock.shardId
                     )
        block1 <- node.addBlock(callDeploy)
        data <- getDataAtPrivateChannel[Effect](
                 block1,
                 calculateDeployUnforgeableName(block1)
               )
        _ = data shouldBe Seq("\"Hello, World!\"")
      } yield ()
    }
  }
}
