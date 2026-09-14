package service

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/utils"
)

type NodeRewardsList struct {
	Epoch uint64
	List  []*NodeReward
}
type NodeRewardsMap map[common.Address]*NodeReward       // nodeAddress(hex with 0x) -> nodeReward
type NodeNewRewardsMap map[common.Address]*NodeNewReward // nodeAddress(hex with 0x) -> nodeNewReward

type NodeReward struct {
	Address                string          `json:"address"` // hex with 0x
	Index                  uint32          `json:"index"`
	TotalRewardAmount      decimal.Decimal `json:"totalRewardAmount"`      // accumulative
	TotalExitDepositAmount decimal.Decimal `json:"totalExitDepositAmount"` // accumulative
	Proof                  string          `json:"proof"`                  // proof of {address/index/totalRewardAmount/totalExitDepositAmount}
	TotalDepositAmount     decimal.Decimal `json:"totalDepositAmount"`     // accumulative, totalDepositAmount >= totalExitDepositAmount
}

type NodeNewReward struct {
	Address                string          `json:"address"` // hex with 0x
	TotalRewardAmount      decimal.Decimal `json:"totalRewardAmount"`
	TotalExitDepositAmount decimal.Decimal `json:"totalExitDepositAmount"`
}

// ensure withdraw and fee already distribute on target epoch
func (s *Service) setMerkleRoot() error {
	dealtEpochOnchain, targetEpoch, targetEth1BlockHeight, shouldGoNext, err := s.checkStateForSetMerkleRoot()
	if err != nil {
		return errors.Wrap(err, "setMerkleRoot checkSyncState failed")
	}
	if !shouldGoNext {
		s.log.Debug("setMerkleRoot should not go next")
		return nil
	}

	var dealtEth1BlockHeight uint64
	preNodeRewardList := NodeRewardsList{}
	if dealtEpochOnchain == 0 {
		// init case
		dealtEth1BlockHeight = s.startAtBlock
	} else {
		preCid, err := s.networkWithdrawContract.NodeRewardsFileCid(nil)
		if err != nil {
			return err
		}

		fileBytes, err := s.dds.DownloadFile(preCid, utils.NodeRewardsFileNameAtEpoch(s.lsdTokenAddress.String(), s.chainID, dealtEpochOnchain))
		if err != nil {
			// 403: a restricted gateway that does not hold this cid. Retry the
			// legacy file name as for 404 rather than aborting the round.
			if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "403") {
				// try old
				if fileBytes, err = s.dds.DownloadFile(preCid, utils.NodeRewardsFileNameAtEpochOld(s.lsdTokenAddress.String(), dealtEpochOnchain)); err != nil {
					return err
				}
			} else {
				return err
			}
		}

		err = json.Unmarshal(fileBytes, &preNodeRewardList)
		if err != nil {
			return err
		}
		if preNodeRewardList.Epoch != dealtEpochOnchain {
			return fmt.Errorf("pre node reward file epoch does not match, cid: %s", preCid)
		}

		dealtEth1BlockHeight, err = s.getEpochStartBlocknumberWithCheck(dealtEpochOnchain)
		if err != nil {
			return err
		}
	}

	preNodeRewardMap := make(NodeRewardsMap)
	for _, nodeReward := range preNodeRewardList.List {
		address := common.HexToAddress(nodeReward.Address)
		_, exist := preNodeRewardMap[address]
		if exist {
			return fmt.Errorf("duplicate node address: %s", nodeReward.Address)
		}
		nodeReward.TotalRewardAmount = nodeReward.TotalRewardAmount.Floor()
		preNodeRewardMap[address] = nodeReward
	}

	newNodeRewardsMap, err := s.getNodeNewRewardsBetween(dealtEth1BlockHeight, targetEth1BlockHeight)
	if err != nil {
		return err
	}

	// cal finalNodeRewardsMap
	finalNodeRewardsMap := make(NodeRewardsMap, 0)
	for _, node := range preNodeRewardMap {
		address := common.HexToAddress(node.Address)
		f, exist := finalNodeRewardsMap[address]
		if exist {
			f.TotalRewardAmount = f.TotalRewardAmount.Add(node.TotalRewardAmount)
			f.TotalExitDepositAmount = f.TotalExitDepositAmount.Add(node.TotalExitDepositAmount)
		} else {
			finalNodeRewardsMap[address] = &NodeReward{
				Address:                node.Address,
				TotalRewardAmount:      node.TotalRewardAmount,
				TotalExitDepositAmount: node.TotalExitDepositAmount,
			}
		}
	}

	for _, node := range newNodeRewardsMap {
		address := common.HexToAddress(node.Address)
		f, exist := finalNodeRewardsMap[address]
		if exist {
			f.TotalRewardAmount = f.TotalRewardAmount.Add(node.TotalRewardAmount)
			f.TotalExitDepositAmount = f.TotalExitDepositAmount.Add(node.TotalExitDepositAmount)
		} else {
			finalNodeRewardsMap[address] = &NodeReward{
				Address:                node.Address,
				TotalRewardAmount:      node.TotalRewardAmount,
				TotalExitDepositAmount: node.TotalExitDepositAmount,
			}
		}
	}

	// cal node totalDepositAmount
	depositedValidators := s.GetValidatorDepositedListBeforeBlock(targetEth1BlockHeight)
	for _, val := range depositedValidators {
		f, exist := finalNodeRewardsMap[val.NodeAddress]
		if exist {
			f.TotalDepositAmount = f.TotalDepositAmount.Add(val.NodeDepositAmountDeci)
		} else {
			finalNodeRewardsMap[val.NodeAddress] = &NodeReward{
				Address:                val.NodeAddress.String(),
				TotalRewardAmount:      decimal.Zero,
				TotalExitDepositAmount: decimal.Zero,
				TotalDepositAmount:     val.NodeDepositAmountDeci,
			}
		}
	}

	// got final reward list
	finalNodeRewardsList := NodeRewardsList{Epoch: targetEpoch, List: make([]*NodeReward, 0)}
	for _, node := range finalNodeRewardsMap {
		// check deposit amount
		if node.TotalExitDepositAmount.GreaterThan(node.TotalDepositAmount) {
			return fmt.Errorf("node %s TotalExitDepositAmount %s GreaterThan TotalDepositAmount %s ",
				node.Address, node.TotalExitDepositAmount.StringFixed(0), node.TotalDepositAmount.StringFixed(0))
		}
		// append
		finalNodeRewardsList.List = append(finalNodeRewardsList.List, node)
	}
	sort.SliceStable(finalNodeRewardsList.List, func(i, j int) bool {
		return finalNodeRewardsList.List[i].Address < finalNodeRewardsList.List[j].Address
	})
	for i, node := range finalNodeRewardsList.List {
		node.Index = uint32(i)
	}

	// call rootHash
	rootHash := utils.NodeHash{}
	if len(finalNodeRewardsList.List) > 0 {
		// build merkle tree
		tree, err := buildMerkleTree(finalNodeRewardsList)
		if err != nil {
			return err
		}
		rootHash, err = tree.GetRootHash()
		if err != nil {
			return err
		}

		// calc proof
		for _, nodeReward := range finalNodeRewardsList.List {
			nodeHash := utils.GetNodeHash(big.NewInt(int64(nodeReward.Index)), common.HexToAddress(nodeReward.Address),
				nodeReward.TotalRewardAmount.BigInt(), nodeReward.TotalExitDepositAmount.BigInt())
			proofList, err := tree.GetProof(nodeHash)
			if err != nil {
				return errors.Wrap(err, "tree.GetProof failed")
			}

			proofStrList := make([]string, len(proofList))
			for i, p := range proofList {
				proofStrList[i] = p.String()
			}
			// set proof
			nodeReward.Proof = strings.Join(proofStrList, ":")
		}
	}

	// upload file
	fileBts, err := json.Marshal(finalNodeRewardsList)
	if err != nil {
		return err
	}
	filePath := utils.NodeRewardsFileNameAtEpoch(s.lsdTokenAddress.String(), s.chainID, targetEpoch)
	cid, err := s.dds.UploadFile(fileBts, filePath)
	if err != nil {
		return err
	}

	var merkleTreeRootHash [32]byte
	copy(merkleTreeRootHash[:], rootHash)

	return s.sendSetMerkleRootTx(int64(targetEpoch), merkleTreeRootHash, cid)
}

func buildMerkleTree(nodelist NodeRewardsList) (*utils.MerkleTree, error) {
	if len(nodelist.List) == 0 {
		return nil, fmt.Errorf("proof list empty")
	}
	list := make(utils.NodeHashList, len(nodelist.List))
	for i, data := range nodelist.List {
		list[i] = utils.GetNodeHash(big.NewInt(int64(data.Index)), common.HexToAddress(data.Address),
			data.TotalRewardAmount.BigInt(), data.TotalExitDepositAmount.BigInt())
	}
	mt := utils.NewMerkleTree(list)
	return mt, nil
}

// check sync and vote state
// return (dealtEpoch,targetEpoch, targetEth1Blocknumber, shouldGoNext, err)
func (s *Service) checkStateForSetMerkleRoot() (uint64, uint64, uint64, bool, error) {
	beaconHead, err := s.connection.BeaconHead()
	if err != nil {
		return 0, 0, 0, false, err
	}

	targetEpoch := (beaconHead.FinalizedEpoch / s.merkleRootDuEpochs) * s.merkleRootDuEpochs

	dealtEpochOnchain := s.latestMerkleRootEpoch
	if err != nil {
		return 0, 0, 0, false, err
	}
	if targetEpoch <= dealtEpochOnchain {
		s.log.Debugf("targetEpoch: %d  dealtEpochOnchain: %d", targetEpoch, dealtEpochOnchain)
		return 0, 0, 0, false, nil
	}

	targetEth1BlockHeight, err := s.getEpochStartBlocknumberWithCheck(targetEpoch)
	if err != nil {
		return 0, 0, 0, false, err
	}

	s.log.WithFields(logrus.Fields{
		"targetEth1BlockHeight":  targetEth1BlockHeight,
		"latestBlockOfSyncBlock": s.latestBlockOfSyncBlock,
		"dealtEpochOnchain":      dealtEpochOnchain,
		"targetEpoch":            targetEpoch,
	}).Debug("setMerkleRoot")

	// wait sync block
	if targetEth1BlockHeight > s.latestBlockOfSyncBlock {
		s.log.Debugf("targetEth1BlockHeight: %d  latestBlockOfSyncBlock: %d", targetEth1BlockHeight, s.latestBlockOfSyncBlock)
		return 0, 0, 0, false, nil
	}

	return dealtEpochOnchain, targetEpoch, targetEth1BlockHeight, true, nil
}

func (s *Service) sendSetMerkleRootTx(targetEpoch int64, rootHash [32]byte, cid string) error {
	err := s.connection.LockAndUpdateTxOpts()
	if err != nil {
		return fmt.Errorf("LockAndUpdateTxOpts err: %w", err)
	}
	defer s.connection.UnlockTxOpts()

	encodeBts, err := s.networkWithdrawAbi.Pack("setMerkleRoot", big.NewInt(targetEpoch), rootHash, cid)
	if err != nil {
		return err
	}

	proposalId := utils.ProposalId(s.networkWithdrawAddress, encodeBts, big.NewInt(targetEpoch))

	// check voted
	hasVoted, err := s.networkProposalContract.HasVoted(nil, proposalId, s.connection.Keypair().CommonAddress())
	if err != nil {
		return fmt.Errorf("networkProposalContract.HasVoted err: %s", err)
	}
	if hasVoted {
		return nil
	}

	s.log.WithFields(logrus.Fields{
		"cid": cid,
	}).Info("will sendSetMerkleRootTx")

	tx, err := s.networkProposalContract.ExecProposal(s.connection.TxOpts(), s.networkWithdrawAddress, encodeBts, big.NewInt(int64(targetEpoch)))
	if err != nil {
		return err
	}

	s.log.Info("send setMerkleRoot tx hash: ", tx.Hash().String())

	return s.waitProposalTxOk(tx.Hash(), proposalId)
}
