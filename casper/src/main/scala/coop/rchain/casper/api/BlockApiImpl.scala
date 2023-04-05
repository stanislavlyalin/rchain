package coop.rchain.casper.api

import cats.data.OptionT
import cats.effect.{Async, Sync}
import cats.syntax.all._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore
import coop.rchain.blockstorage.BlockStore.BlockStore
import coop.rchain.blockstorage.dag.BlockDagStorage.DeployId
import coop.rchain.blockstorage.dag._
import coop.rchain.casper._
import coop.rchain.casper.api.BlockApi._
import coop.rchain.casper.api.GraphGenerator.ValidatorBlock
import coop.rchain.casper.blocks.proposer.ProposeResult._
import coop.rchain.casper.blocks.proposer._
import coop.rchain.casper.genesis.contracts.StandardDeploys
import coop.rchain.casper.protocol._
import coop.rchain.casper.protocol.deploy.v1.{
  DeployExecStatus,
  NotProcessed,
  ProcessedWithError,
  ProcessedWithSuccess
}
import coop.rchain.casper.rholang.RuntimeManager
import coop.rchain.casper.state.instances.ProposerState
import coop.rchain.casper.syntax._
import coop.rchain.casper.util._
import coop.rchain.comm.PeerNode
import coop.rchain.comm.rp.Connect.Connections
import coop.rchain.crypto.signatures.Signed
import coop.rchain.graphz._
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.models.BlockHash.BlockHash
import coop.rchain.models.rholang.RhoType.RhoDeployId
import coop.rchain.models.rholang.sorter.Sortable._
import coop.rchain.models.serialization.implicits._
import coop.rchain.models.syntax._
import coop.rchain.models.{BlockMetadata, Par}
import coop.rchain.rspace.hashing.StableHashProvider
import coop.rchain.rspace.trace.{COMM, Consume, Produce}
import coop.rchain.sdk.syntax.all._
import coop.rchain.shared.Log
import coop.rchain.shared.syntax._
import fs2.Stream

import scala.collection.immutable.SortedMap
import cats.effect.Ref

object BlockApiImpl {
  def apply[F[_]: Async: RuntimeManager: BlockDagStorage: BlockStore: Log: Span](
      validatorOpt: Option[ValidatorIdentity],
      networkId: String,
      shardId: String,
      minPhloPrice: Long,
      version: String,
      networkStatus: F[(PeerNode, Connections, Seq[PeerNode])],
      isNodeReadOnly: Boolean,
      maxDepthLimit: Int,
      devMode: Boolean,
      triggerPropose: Option[ProposeFunction[F]],
      proposerStateRefOpt: Option[Ref[F, ProposerState[F]]],
      autoPropose: Boolean,
      executionTracker: StatefulExecutionTracker[F]
  ): F[BlockApiImpl[F]] =
    Sync[F].delay(
      new BlockApiImpl(
        validatorOpt,
        networkId,
        shardId,
        minPhloPrice,
        version,
        networkStatus,
        isNodeReadOnly,
        maxDepthLimit,
        devMode,
        triggerPropose,
        proposerStateRefOpt,
        autoPropose,
        executionTracker
      )
    )

  sealed trait LatestBlockMessageError     extends Throwable
  final case object ValidatorReadOnlyError extends LatestBlockMessageError
  final case object NoBlockMessageError    extends LatestBlockMessageError

  // TODO: we should refactor BlockApi with applicative errors for better classification
  //  of errors and to overcome nesting when validating data.
  final case class BlockRetrievalError(message: String) extends Exception
}

class BlockApiImpl[F[_]: Async: RuntimeManager: BlockDagStorage: BlockStore: Log: Span](
    validatorOpt: Option[ValidatorIdentity],
    networkId: String,
    shardId: String,
    minPhloPrice: Long,
    version: String,
    networkStatus: F[(PeerNode, Connections, Seq[PeerNode])],
    isNodeReadOnly: Boolean,
    maxDepthLimit: Int,
    devMode: Boolean,
    triggerProposeOpt: Option[ProposeFunction[F]],
    proposerStateRefOpt: Option[Ref[F, ProposerState[F]]],
    autoPropose: Boolean,
    executionTracker: StatefulExecutionTracker[F]
) extends BlockApi[F] {
  import BlockApiImpl._

  val blockAPIMetricsSource: Metrics.Source = Metrics.Source(Metrics.BaseSource, "block-api")
  val deploySource: Metrics.Source          = Metrics.Source(blockAPIMetricsSource, "deploy")
  val getBlockSource: Metrics.Source        = Metrics.Source(blockAPIMetricsSource, "get-block")

  override def status: F[Status] =
    for {
      netInfo                  <- networkStatus
      (thisNode, peers, nodes) = netInfo
      status = Status(
        version = VersionInfo(api = 1.toString, node = version),
        thisNode.toAddress,
        networkId,
        shardId,
        peers = peers.length,
        nodes = nodes.length,
        minPhloPrice
      )
    } yield status

  override def deploy(deploy: Signed[DeployData]): F[ApiErr[String]] = Span[F].trace(deploySource) {
    def casperDeploy: F[ApiErr[String]] =
      for {
        r <- MultiParentCasper
              .deploy(deploy)
              .map(
                _.bimap(
                  err => err.details,
                  res => s"Success!\nDeployId is: ${PrettyPrinter.buildStringNoLimit(res)}"
                )
              )
        // Call a propose if autoPropose flag is on
        _ <- triggerProposeOpt.traverse(_(true)) whenA autoPropose
      } yield r

    // Check if node is read-only
    val readOnlyError = new RuntimeException(
      "Deploy was rejected because node is running in read-only mode."
    ).raiseError[F, ApiErr[String]]
    val readOnlyCheck = readOnlyError.whenA(isNodeReadOnly)

    // Check if deploy's shardId equals to node shardId
    val shardIdError = new RuntimeException(
      s"Deploy shardId '${deploy.data.shardId}' is not as expected network shard '$shardId'."
    ).raiseError[F, ApiErr[String]]
    val shardIdCheck = shardIdError.whenA(deploy.data.shardId != shardId)

    // Check if deploy is signed with system keys
    val isForbiddenKey = StandardDeploys.systemPublicKeys.contains(deploy.pk)
    val forbiddenKeyError = new RuntimeException(
      s"Deploy refused because it's signed with forbidden private key."
    ).raiseError[F, ApiErr[String]]
    val forbiddenKeyCheck = forbiddenKeyError.whenA(isForbiddenKey)

    // Check if deploy has minimum phlo price
    val minPriceError = new RuntimeException(
      s"Phlo price ${deploy.data.phloPrice} is less than minimum price $minPhloPrice."
    ).raiseError[F, ApiErr[String]]
    val minPhloPriceCheck = minPriceError.whenA(deploy.data.phloPrice < minPhloPrice)

    readOnlyCheck >> shardIdCheck >> forbiddenKeyCheck >> minPhloPriceCheck >> casperDeploy
  }

  override def deployStatus(deployId: DeployId): F[ApiErr[DeployExecStatus]] = {
    def notProcessed(status: String): DeployExecStatus =
      DeployExecStatus().withNotProcessed(NotProcessed(status))

    def findProcessedDeployResultAndStatus: OptionT[F, DeployExecStatus] = {
      val lookupDeploy = OptionT(BlockDagStorage[F].lookupByDeployId(deployId))
      lookupDeploy.semiflatMap { blockHash =>
        val deployIdCh = RhoDeployId(deployId.toByteArray)
        for {
          block     <- BlockStore[F].getUnsafe(blockHash)
          deployOpt = block.state.deploys.find(_.deploy.sig == deployId)
          deploy <- deployOpt.liftTo {
                     val blockHashStr = PrettyPrinter.buildString(blockHash)
                     val deploySigStr = PrettyPrinter.buildString(deployId)
                     val errMsg =
                       s"Deploy not found in the block, blockHash: $blockHashStr, deploy sig: $deploySigStr"
                     new Exception(errMsg)
                   }
          status <- if (!deploy.isFailed)
                     // For successfully executed deploy, get the result and block info
                     for {
                       data              <- getDataAtParRaw(deployIdCh, blockHash)
                       (par, lightBlock) = data
                       success           = ProcessedWithSuccess(par, lightBlock)
                     } yield DeployExecStatus().withProcessedWithSuccess(success)
                   else
                     for {
                       // For failed deploy, get the error and block info
                       deployStatusOpt <- executionTracker.findDeploy(deployId)
                       lightBlock      = getLightBlockInfo(block)
                       errMsg = deployStatusOpt
                         .collect { case DeployStatusError(errorMsg) => errorMsg }
                         .getOrElse(
                           "<deploy error message not available in cache or deploy executed on another node>"
                         )
                       error = ProcessedWithError(errMsg, lightBlock)
                     } yield DeployExecStatus().withProcessedWithError(error)
        } yield status
      }
    }

    def findPooledDeploy: OptionT[F, DeployExecStatus] = {
      val deployPooled = BlockDagStorage[F].containsDeployInPool(deployId).map(_.guard[Option])
      // Deploy found in the pool, waiting to be executed and added to a block
      OptionT(deployPooled).as(notProcessed("Pooled"))
    }

    def findCurrentlyExecutedDeploy: OptionT[F, DeployExecStatus] = {
      val deployStatusOptT = OptionT(executionTracker.findDeploy(deployId))
      // Status found, block creation in progress, deploy execution started
      deployStatusOptT.as(notProcessed("Running"))
    }

    def findPooledOrRunningDeploy: OptionT[F, DeployExecStatus] =
      findPooledDeploy.semiflatMap { pulled =>
        // Find if pooled deploy is running, if not return pooled
        findCurrentlyExecutedDeploy.getOrElse(pulled)
      }

    // Deploy not found in the system, unknown
    def unknownDeploy: DeployExecStatus = notProcessed("Unknown")

    // Find deploy status, result, error, ...
    // 1. find deploy added to a block
    // 2. find deploy in pool or in execution
    // 3. return unknown for rest (in API v2 this should be error 404)
    val result = findProcessedDeployResultAndStatus orElse findPooledOrRunningDeploy getOrElse unknownDeploy

    result.attempt.map(_.leftMap(_.getMessageSafe))
  }

  override def createBlock(isAsync: Boolean = false): F[ApiErr[String]] = {
    def logDebug(err: String)  = Log[F].debug(err) >> err.asLeft[String].pure[F]
    def logSucess(msg: String) = Log[F].info(msg) >> msg.asRight[Error].pure[F]
    for {
      // Trigger propose
      triggerPropose <- triggerProposeOpt.liftTo(new Exception("Propose error: read-only node."))
      proposerResult <- triggerPropose(isAsync)
      r <- proposerResult match {
            case ProposerEmpty =>
              logDebug(s"Failure: another propose is in progress")
            case ProposerFailure(status, seqNumber) =>
              logDebug(s"Failure: $status (seqNum $seqNumber)")
            case ProposerStarted(seqNumber) =>
              logSucess(s"Propose started (seqNum $seqNumber)")
            case ProposerSuccess(_, block) =>
              // TODO: [WARNING] Format of this message is hardcoded in pyrchain when checking response result
              //  Fix to use structured result with transport errors/codes.
              // https://github.com/rchain/pyrchain/blob/a2959c75bf/rchain/client.py#L42
              val blockHashHex = block.blockHash.toHexString
              logSucess(s"Success! Block $blockHashHex created and added.")
          }
    } yield r
  }

  // Get result of the propose
  override def getProposeResult: F[ApiErr[String]] =
    for {
      proposerState <- proposerStateRefOpt.liftTo(new Exception("Error: read-only node."))
      pr            <- proposerState.get.map(_.currProposeResult)
      r <- pr match {
            // return latest propose result
            case None =>
              for {
                result <- proposerState.get.map(
                           _.latestProposeResult.getOrElse(ProposeResult.notEnoughBlocks, None)
                         )
                msg = result._2 match {
                  case Some(block) =>
                    s"Success! Block ${block.blockHash.toHexString} created and added."
                      .asRight[Error]
                  case None => s"${result._1.proposeStatus.show}".asLeft[String]
                }
              } yield msg
            // wait for current propose to finish and return result
            case Some(resultDef) =>
              for {
                // this will hang API call until propose is complete, and then return result
                // TODO cancel this get when connection drops
                result <- resultDef.get
                msg = result._2 match {
                  case Some(block) =>
                    s"Success! Block ${block.blockHash.toHexString} created and added."
                      .asRight[Error]
                  case None => s"${result._1.proposeStatus.show}".asLeft[String]
                }
              } yield msg
          }
    } yield r

  /**
    * Performs transformation on a stream of blocks (from highest height, same heights loaded as batch)
    *
    * @param heightMap height map to iterate block hashes
    * @param transform function to run for each block
    * @return stream of transform results
    */
  private def getFromBlocks[A](
      heightMap: SortedMap[Long, Set[BlockHash]]
  )(transform: BlockMessage => F[A]): Stream[F, List[A]] = {
    // TODO: check if conversion of SortedMap#toIndexedSeq is performant enough
    val reverseHeightMap = heightMap.toIndexedSeq.reverse
    val iterBlockHashes  = reverseHeightMap.iterator.map(_._2.toList)
    Stream
      .fromIterator(iterBlockHashes)
      .evalMap(_.traverse(BlockStore[F].getUnsafe))
      .evalMap(_.traverse(transform))
  }

  override def getListeningNameDataResponse(
      depth: Int,
      listeningName: Par
  ): F[ApiErr[(Seq[DataWithBlockInfo], Int)]] = {
    val response: F[Either[Error, (Seq[DataWithBlockInfo], Int)]] = for {
      dag                 <- BlockDagStorage[F].getRepresentation
      heightMap           = dag.heightMap
      depthWithLimit      = Math.min(depth, maxDepthLimit).toLong
      sortedListeningName <- parSortable.sortMatch[F](listeningName).map(_.term)
      blockDataStream = getFromBlocks(heightMap) { block =>
        getDataWithBlockInfo(sortedListeningName, block)
      }
      // For compatibility with v0.12.x depth must include all blocks with the same height
      //  e.g. depth=1 should always return latest block created by the node
      blocksWithActiveName <- blockDataStream
                               .map(_.flatten)
                               .take(depthWithLimit)
                               .compile
                               .toList
                               .map(_.flatten)
    } yield (blocksWithActiveName, blocksWithActiveName.length).asRight[String]
    // Check depth limit
    if (depth > maxDepthLimit)
      s"Your request on getListeningName depth $depth exceed the max limit $maxDepthLimit"
        .asLeft[(Seq[DataWithBlockInfo], Int)]
        .pure[F]
    else response
  }

  override def getListeningNameContinuationResponse(
      depth: Int,
      listeningNames: Seq[Par]
  ): F[ApiErr[(Seq[ContinuationsWithBlockInfo], Int)]] = {
    val response: F[Either[Error, (Seq[ContinuationsWithBlockInfo], Int)]] = for {
      dag            <- BlockDagStorage[F].getRepresentation
      heightMap      = dag.heightMap
      depthWithLimit = Math.min(depth, maxDepthLimit).toLong
      sortedListeningNames <- listeningNames.toList
                               .traverse(parSortable.sortMatch[F](_).map(_.term))
      blockDataStream = getFromBlocks(heightMap) { block =>
        getContinuationsWithBlockInfo(sortedListeningNames, block)
      }
      // For compatibility with v0.12.x depth must include all blocks with the same height
      //  e.g. depth=1 should always return latest block created by the node
      blocksWithActiveName <- blockDataStream
                               .map(_.flatten)
                               .take(depthWithLimit)
                               .compile
                               .toList
                               .map(_.flatten)
    } yield (blocksWithActiveName, blocksWithActiveName.length).asRight[String]
    // Check depth limit
    if (depth > maxDepthLimit)
      s"Your request on getListeningNameContinuation depth $depth exceed the max limit $maxDepthLimit"
        .asLeft[(Seq[ContinuationsWithBlockInfo], Int)]
        .pure[F]
    else response
  }

  private def getDataWithBlockInfo(
      sortedListeningName: Par,
      block: BlockMessage
  ): F[Option[DataWithBlockInfo]] =
    // TODO: For Produce it doesn't make sense to have multiple names
    if (isListeningNameReduced(block, Seq(sortedListeningName))) {
      val stateHash = block.postStateHash
      for {
        data      <- RuntimeManager[F].getData(stateHash)(sortedListeningName)
        blockInfo = getLightBlockInfo(block)
      } yield Option[DataWithBlockInfo](DataWithBlockInfo(data, blockInfo))
    } else {
      none[DataWithBlockInfo].pure[F]
    }

  private def getContinuationsWithBlockInfo(
      sortedListeningNames: Seq[Par],
      block: BlockMessage
  ): F[Option[ContinuationsWithBlockInfo]] =
    if (isListeningNameReduced(block, sortedListeningNames)) {
      val stateHash = block.postStateHash
      for {
        continuations <- RuntimeManager[F].getContinuation(stateHash)(sortedListeningNames)
        continuationInfos = continuations.map(
          continuation => WaitingContinuationInfo(continuation._1, continuation._2)
        )
        blockInfo = getLightBlockInfo(block)
      } yield Option[ContinuationsWithBlockInfo](
        ContinuationsWithBlockInfo(continuationInfos, blockInfo)
      )
    } else {
      none[ContinuationsWithBlockInfo].pure[F]
    }

  private def isListeningNameReduced(
      block: BlockMessage,
      sortedListeningName: Seq[Par]
  ): Boolean = {
    val eventLog = block.state.deploys.flatMap(_.deployLog).map(EventConverter.toRspaceEvent)
    eventLog.exists {
      case Produce(channelHash, _, _) =>
        assert(sortedListeningName.size == 1, "Produce can have only one channel")
        channelHash == StableHashProvider.hash(sortedListeningName.head)
      case Consume(channelsHashes, _, _) =>
        channelsHashes.toList.sorted == sortedListeningName
          .map(StableHashProvider.hash(_))
          .toList
          .sorted
      case COMM(consume, produces, _, _) =>
        (consume.channelsHashes.toList.sorted ==
          sortedListeningName.map(StableHashProvider.hash(_)).toList.sorted) ||
          produces.exists(
            produce => produce.channelsHash == StableHashProvider.hash(sortedListeningName)
          )
    }
  }

  private def toposortDag[A](depth: Int, maxDepthLimit: Int)(
      doIt: Vector[Vector[BlockHash]] => F[ApiErr[A]]
  ): F[ApiErr[A]] = {
    def response: F[ApiErr[A]] =
      for {
        dag               <- BlockDagStorage[F].getRepresentation
        latestBlockNumber = dag.latestBlockNumber
        topoSort          <- dag.topoSortUnsafe(latestBlockNumber - depth, none)
        result            <- doIt(topoSort)
      } yield result

    if (depth > maxDepthLimit)
      s"Your request depth $depth exceed the max limit $maxDepthLimit".asLeft[A].pure[F]
    else
      response
  }

  override def getBlocksByHeights(
      startBlockNumber: Long,
      endBlockNumber: Long
  ): F[ApiErr[List[LightBlockInfo]]] = {
    def response: F[ApiErr[List[LightBlockInfo]]] =
      for {
        dag         <- BlockDagStorage[F].getRepresentation
        topoSortDag <- dag.topoSortUnsafe(startBlockNumber, Some(endBlockNumber))
        result <- topoSortDag
                   .foldM(List.empty[LightBlockInfo]) {
                     case (blockInfosAtHeightAcc, blockHashesAtHeight) =>
                       for {
                         blocksAtHeight     <- blockHashesAtHeight.traverse(BlockStore[F].getUnsafe)
                         blockInfosAtHeight = blocksAtHeight.map(getLightBlockInfo)
                       } yield blockInfosAtHeightAcc ++ blockInfosAtHeight
                   }
                   .map(_.asRight[Error])
      } yield result

    if (endBlockNumber - startBlockNumber > maxDepthLimit)
      s"Your request startBlockNumber $startBlockNumber and endBlockNumber $endBlockNumber exceed the max limit $maxDepthLimit"
        .asLeft[List[LightBlockInfo]]
        .pure[F]
    else
      response
  }

  override def visualizeDag[R](
      depth: Int,
      startBlockNumber: Int,
      showJustificationLines: Boolean
  ): F[ApiErr[Vector[String]]] =
    for {
      dag <- BlockDagStorage[F].getRepresentation
      // the default startBlockNumber is 0
      // if the startBlockNumber is 0 , it would use the latestBlockNumber for backward compatible
      startBlockNum = if (startBlockNumber == 0) dag.latestBlockNumber else startBlockNumber.toLong
      depthLimited  = if (depth <= 0 || depth > maxDepthLimit) maxDepthLimit else depth
      ref           <- Ref[F].of(Vector[String]())
      ser           = new ListSerializer(ref)
      lowestHeight  = startBlockNum - depthLimited
      topBlocks     = dag.dagMessageState.msgMap.valuesIterator.filter(_.height >= lowestHeight)
      blocks = topBlocks.map { m =>
        def toHashStr(blockHash: BlockHash) = blockHash.toHexString.take(5)
        val blockHashStr                    = toHashStr(m.id)
        val parentsStr                      = m.parents.map(toHashStr).toList
        val fringeStr                       = m.fringe.map(toHashStr)
        val validatorStr                    = toHashStr(m.sender)
        ValidatorBlock(blockHashStr, validatorStr, m.height, parentsStr, fringeStr)
      }.toVector
      _      <- GraphGenerator.dagAsCluster[F](blocks, ser)
      result <- ref.get
    } yield result.asRight[Error]

  override def machineVerifiableDag(depth: Int): F[ApiErr[String]] =
    toposortDag[String](depth, maxDepthLimit) { topoSort =>
      val fetchParents: BlockHash => F[List[BlockHash]] = { blockHash =>
        BlockStore[F].getUnsafe(blockHash) map (_.justifications)
      }

      MachineVerifiableDag[F](topoSort, fetchParents)
        .map(_.map(edges => edges.show).mkString("\n"))
        .map(_.asRight[Error])
    }

  override def getBlocks(depth: Int): F[ApiErr[List[LightBlockInfo]]] =
    toposortDag[List[LightBlockInfo]](depth, maxDepthLimit) { topoSort =>
      topoSort
        .foldM(List.empty[LightBlockInfo]) {
          case (blockInfosAtHeightAcc, blockHashesAtHeight) =>
            for {
              blocksAtHeight     <- blockHashesAtHeight.traverse(BlockStore[F].getUnsafe)
              blockInfosAtHeight = blocksAtHeight.map(getLightBlockInfo)
            } yield blockInfosAtHeightAcc ++ blockInfosAtHeight
        }
        .map(_.reverse.asRight[Error])
    }

  override def findDeploy(id: DeployId): F[ApiErr[LightBlockInfo]] =
    for {
      maybeBlockHash <- BlockDagStorage[F].lookupByDeployId(id)
      maybeBlock     <- maybeBlockHash.traverse(BlockStore[F].getUnsafe)
      response       = maybeBlock.map(getLightBlockInfo)
    } yield response.fold(
      s"Couldn't find block containing deploy with id: ${PrettyPrinter
        .buildStringNoLimit(id)}".asLeft[LightBlockInfo]
    )(_.asRight)

  override def getBlock(hash: String): F[ApiErr[BlockInfo]] = Span[F].trace(getBlockSource) {
    val response = for {
      // Add constraint on the length of searched hash to prevent to many block results
      // which can cause severe CPU load.
      _ <- BlockRetrievalError(s"Input hash value must be at least 6 characters: $hash")
            .raiseError[F, ApiErr[BlockInfo]]
            .whenA(hash.length < 6)
      // Check if hash string is in Base16 encoding and convert to ByteString
      hashByteString <- hash.hexToByteString
                         .liftTo[F](
                           BlockRetrievalError(
                             s"Input hash value is not valid hex string: $hash"
                           )
                         )
      // Check if hash is complete and not just the prefix in which case
      // we can use `get` directly and not iterate over the whole block hash index.
      getBlock  = BlockStore[F].get1(hashByteString)
      findBlock = findBlockFromStore(hash)
      blockOpt  <- if (hash.length == 64) getBlock else findBlock
      // Get block form the block store
      block <- blockOpt.liftTo(
                BlockRetrievalError(s"Error: Failure to find block with hash: $hash")
              )
      // Check if the block is added to the dag and convert it to block info
      dag <- BlockDagStorage[F].getRepresentation
      blockInfo <- if (dag.contains(block.blockHash)) Sync[F].delay(getFullBlockInfo(block))
                  else
                    BlockRetrievalError(s"Error: Block with hash $hash received but not added yet")
                      .raiseError[F, BlockInfo]

    } yield blockInfo

    response.map(_.asRight[String]).handleError {
      // Convert error message from BlockRetrievalError
      case BlockRetrievalError(errorMessage) => errorMessage.asLeft[BlockInfo]
    }
  }

  // Be careful to use this method , because it would iterate the whole indexes to find the matched one which would cause performance problem
  // Trying to use BlockStore.get as much as possible would more be preferred
  private def findBlockFromStore(hash: String): F[Option[BlockMessage]] =
    for {
      dag          <- BlockDagStorage[F].getRepresentation
      blockHashOpt = dag.find(hash)
      message      <- blockHashOpt.flatTraverse(BlockStore[F].get1)
    } yield message

  override def lastFinalizedBlock: F[ApiErr[BlockInfo]] =
    for {
      dag                <- BlockDagStorage[F].getRepresentation
      lastFinalizedBlock <- dag.lastFinalizedBlockUnsafe.flatMap(BlockStore[F].getUnsafe)
      blockInfo          = getFullBlockInfo(lastFinalizedBlock)
    } yield blockInfo.asRight

  override def isFinalized(hash: String): F[ApiErr[Boolean]] =
    for {
      dag            <- BlockDagStorage[F].getRepresentation
      givenBlockHash = hash.unsafeHexToByteString
      result         = dag.isFinalized(givenBlockHash)
    } yield result.asRight[Error]

  override def bondStatus(
      publicKey: ByteString,
      targetBlock: Option[BlockMessage] = none[BlockMessage]
  ): F[ApiErr[Boolean]] =
    for {
      dag                <- BlockDagStorage[F].getRepresentation
      lastFinalizedBlock <- dag.lastFinalizedBlockUnsafe.flatMap(BlockStore[F].getUnsafe)
      postStateHash      = targetBlock.getOrElse(lastFinalizedBlock).postStateHash
      bonds              <- RuntimeManager[F].computeBonds(postStateHash)
      validatorBondOpt   = bonds.get(publicKey)
    } yield validatorBondOpt.isDefined.asRight[Error]

  /**
    * Explore the data or continuation in the tuple space for specific blockHash
    *
    * @param term: the term you want to explore in the request. Be sure the first new should be `return`
    * @param blockHash: the block hash you want to explore
    * @param usePreStateHash: Each block has preStateHash and postStateHash. If usePreStateHash is true, the explore
    *                       would try to execute on preState.
    * */
  override def exploratoryDeploy(
      term: String,
      blockHash: Option[String] = none,
      usePreStateHash: Boolean = false
  ): F[ApiErr[(Seq[Par], LightBlockInfo)]] =
    if (isNodeReadOnly || devMode) {
      for {
        dag <- BlockDagStorage[F].getRepresentation
        targetBlock <- if (blockHash.isEmpty)
                        dag.lastFinalizedBlockUnsafe.flatMap(BlockStore[F].get1)
                      else
                        for {
                          hashByteString <- blockHash
                                             .getOrElse("")
                                             .hexToByteString
                                             .liftTo[F](
                                               BlockRetrievalError(
                                                 s"Input hash value is not valid hex string: $blockHash"
                                               )
                                             )
                          block <- BlockStore[F].get1(hashByteString)
                        } yield block
        res <- targetBlock.traverse(b => {
                val postStateHash =
                  if (usePreStateHash) b.preStateHash
                  else b.postStateHash
                for {
                  res            <- RuntimeManager[F].playExploratoryDeploy(term, postStateHash)
                  lightBlockInfo = getLightBlockInfo(b)
                } yield (res, lightBlockInfo)
              })
      } yield res.fold(
        s"Can not find block $blockHash".asLeft[(Seq[Par], LightBlockInfo)]
      )(_.asRight[Error])
    } else {
      "Exploratory deploy can only be executed on read-only RNode."
        .asLeft[(Seq[Par], LightBlockInfo)]
        .pure[F]
    }

  override def getLatestMessage: F[ApiErr[BlockMetadata]] =
    for {
      dag            <- BlockDagStorage[F].getRepresentation
      validator      <- validatorOpt.liftTo[F](ValidatorReadOnlyError)
      validatorBytes = ByteString.copyFrom(validator.publicKey.bytes)
      // TODO: return list of latest messages for a validator
      //  - this is possible with invalid blocks
      latestMessageOpt <- dag.dagMessageState.latestMsgs
                           .filter(_.sender == validatorBytes)
                           .map(_.id)
                           .headOption
                           .traverse(BlockDagStorage[F].lookupUnsafe(_))
      latestMessage <- latestMessageOpt.liftTo[F](NoBlockMessageError)
    } yield latestMessage.asRight[Error]

  override def getDataAtPar(
      par: Par,
      blockHashHex: String,
      usePreStateHash: Boolean
  ): F[ApiErr[(Seq[Par], LightBlockInfo)]] =
    Sync[F]
      .defer {
        for {
          blockHash <- blockHashHex.hexToByteString.liftTo(
                        new Exception(s"Invalid block hash base 16 encoding, $blockHashHex")
                      )
          result <- getDataAtParRaw(par, blockHash, usePreStateHash)
        } yield result
      }
      .attempt
      .map(_.leftMap(_.getMessageSafe))

  private def getDataAtParRaw(
      par: Par,
      blockHash: ByteString,
      usePreStateHash: Boolean = false
  ): F[(Seq[Par], LightBlockInfo)] =
    for {
      block     <- BlockStore[F].getUnsafe(blockHash)
      sortedPar <- parSortable.sortMatch[F](par).map(_.term)
      stateHash = if (usePreStateHash) block.preStateHash
      else block.postStateHash
      data <- RuntimeManager[F].getData(stateHash)(sortedPar)
      lbi  = getLightBlockInfo(block)
    } yield (data, lbi)
}
