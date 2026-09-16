package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	deposit_contract "github.com/stafiprotocol/eth-lsd-relay/bindings/DepositContract"
	erc20 "github.com/stafiprotocol/eth-lsd-relay/bindings/Erc20"
	fee_pool "github.com/stafiprotocol/eth-lsd-relay/bindings/FeePool"
	lsd_network_factory "github.com/stafiprotocol/eth-lsd-relay/bindings/LsdNetworkFactory"
	network_balances "github.com/stafiprotocol/eth-lsd-relay/bindings/NetworkBalances"
	network_proposal "github.com/stafiprotocol/eth-lsd-relay/bindings/NetworkProposal"
	network_withdraw "github.com/stafiprotocol/eth-lsd-relay/bindings/NetworkWithdraw"
	node_deposit "github.com/stafiprotocol/eth-lsd-relay/bindings/NodeDeposit"
	user_deposit "github.com/stafiprotocol/eth-lsd-relay/bindings/UserDeposit"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/config"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/connection"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/connection/beacon"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/destorage"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/destorage/pinata"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/local_store"
	"github.com/stafiprotocol/eth-lsd-relay/pkg/utils"
	"github.com/stratisproject/prysm-stratis/beacon-chain/core/signing"
	"github.com/stratisproject/prysm-stratis/config/params"
)

type Service struct {
	stop             chan struct{}
	startServiceOnce sync.Once
	log              *logrus.Entry
	manager          *ServiceManager

	submitBalancesDuEpochs        uint64
	distributeWithdrawalsDuEpochs uint64
	distributePriorityFeeDuEpochs uint64
	merkleRootDuEpochs            uint64

	batchRequestBlocksNumber uint64

	connection          *connection.CachedConnection
	dds                 destorage.DeStorage
	eth2Config          beacon.Eth2Config
	chainID             uint64
	withdrawCredentials []byte
	domain              []byte // for eth2 sigs

	lsdNetworkFactoryAddress common.Address
	lsdTokenAddress          common.Address
	feePoolAddress           common.Address
	networkWithdrawAddress   common.Address
	networkBalancesAddress   common.Address
	nodeDepositAddress       common.Address

	networkWithdrawAbi abi.ABI
	networkBalancesAbi abi.ABI
	nodeDepositAbi     abi.ABI

	lsdNetworkFactoryContract *lsd_network_factory.LsdNetworkFactory
	nodeDepositContract       *node_deposit.CustomNodeDeposit
	networkWithdrawContract   *network_withdraw.NetworkWithdraw
	govDepositContract        *deposit_contract.DepositContract
	networkProposalContract   *network_proposal.NetworkProposal
	networkBalancesContract   *network_balances.NetworkBalances
	lsdTokenContract          *erc20.Erc20
	userDepositContract       *user_deposit.UserDeposit
	feePoolContract           *fee_pool.FeePool

	nodeCommissionRate     decimal.Decimal
	platformCommissionRate decimal.Decimal

	latestSlotOfSyncBlock   uint64
	latestBlockOfSyncBlock  uint64
	waitFirstNodeStakeEvent bool
	localSyncedBlockHeight  uint64
	localStore              *local_store.LocalStore

	latestBlockOfSyncEvents      uint64
	latestBlockOfUpdateValidator uint64
	latestEpochOfUpdateValidator uint64
	startAtBlock                 uint64

	cycleSeconds                      uint64
	latestDistributeWithdrawalsHeight uint64
	latestDistributePriorityFeeHeight uint64
	latestMerkleRootEpoch             uint64

	govDeposits map[string][][]byte // pubkey(hex.encodeToString) -> withdrawalCredentials

	validators             map[string]*Validator // pubkey(hex.encodeToString) -> validator
	validatorsByIndex      map[uint64]*Validator // validator index -> validator
	validatorsByIndexMutex sync.RWMutex

	nodes map[common.Address]*Node // nodeAddress -> node

	stakerWithdrawals map[uint64]*StakerWithdrawal // withraw index => stakerWithdrawal

	minExecutionBlockHeight uint64

	cacheEpochToBlockID      *lru.Cache[uint64, uint64]
	cacheEpochToBlockIDMutex sync.RWMutex

	exitElections map[uint64]*ExitElection // cycle -> exitElection
}

type Node struct {
	NodeAddress  common.Address
	NodeType     uint8 // 1 light node 2 trust node
	PubkeyNumber uint64
}
type Validator struct {
	Pubkey []byte

	NodeAddress           common.Address
	DepositSignature      []byte
	NodeDepositAmountDeci decimal.Decimal // decimals 18
	NodeDepositAmount     uint64          //decimals 9
	DepositBlock          uint64
	ActiveEpoch           uint64
	EligibleEpoch         uint64
	ExitEpoch             uint64
	WithdrawableEpoch     uint64
	NodeType              uint8  // 1 light node 2 trust node
	ValidatorIndex        uint64 // Notice!!!!!!: validator index is zero before status waiting

	Balance          uint64 // realtime balance
	EffectiveBalance uint64 // realtime effectiveBalance
	Status           uint8  // status details defined in pkg/utils/eth2.go
}

type StakerWithdrawal struct {
	WithdrawIndex uint64

	Address            common.Address
	EthAmount          decimal.Decimal
	BlockNumber        uint64
	ClaimedBlockNumber uint64
}

type ExitElection struct {
	WithdrawCycle      uint64
	ValidatorIndexList []uint64
}

type CachedBeaconBlock struct {
	BeaconBlockId        uint64
	ExecutionBlockNumber uint64
	ProposerIndex        uint64
	Withdrawals          []*CachedWithdrawal
}

type CachedTransaction struct {
	// big endian
	Recipient []byte
	// big endian
	Amount []byte
}

type CachedWithdrawal struct {
	Address        string
	ValidatorIndex uint64
	Amount         uint64
}

type Handler struct {
	method func() error
	name   string
}

func NewService(
	cfg *config.Config,
	manager *ServiceManager,
	conn *connection.CachedConnection,
	localStore *local_store.LocalStore,
) (*Service, error) {
	if !common.IsHexAddress(cfg.Contracts.LsdTokenAddress) {
		return nil, fmt.Errorf("LsdTokenAddress contract address fmt err")
	}
	if !common.IsHexAddress(cfg.Contracts.LsdFactoryAddress) {
		return nil, fmt.Errorf("LsdFactoryAddress contract address fmt err")
	}

	err := os.MkdirAll(cfg.LogFilePath, 0700)
	if err != nil {
		return nil, fmt.Errorf("LogFilePath %w", err)
	}

	if isDir, err := utils.IsDir(cfg.LogFilePath); err != nil {
		return nil, err
	} else if !isDir {
		return nil, fmt.Errorf("logFilePath %s is not dir", cfg.LogFilePath)
	}
	if cfg.BatchRequestBlocksNumber == 0 {
		return nil, fmt.Errorf("BatchRequestBlocksNumber is zero")
	}

	info, err := localStore.Read(cfg.Contracts.LsdTokenAddress)
	if err != nil {
		return nil, err
	}
	var localSyncedBlockHeight uint64 = 0
	if info != nil {
		localSyncedBlockHeight = info.SyncedHeight
	}
	log := logrus.WithFields(logrus.Fields{
		"lsdToken": cfg.Contracts.LsdTokenAddress,
	})

	dds, err := pinata.NewClient(
		cfg.Pinata.Endpoint,
		cfg.Pinata.Gateways,
		cfg.Pinata.Apikey,
	)
	if err != nil {
		return nil, fmt.Errorf("fail to new pinata client: %w", err)
	}
	dds.StartUnpinFiles(utils.Day * time.Duration(cfg.Pinata.PinDays))

	cacheEpochToBlockID, err := lru.New[uint64, uint64](1024 * 1000)
	if err != nil {
		return nil, err
	}

	s := &Service{
		stop:                     make(chan struct{}),
		manager:                  manager,
		connection:               conn,
		log:                      log,
		dds:                      dds,
		lsdTokenAddress:          common.HexToAddress(cfg.Contracts.LsdTokenAddress),
		lsdNetworkFactoryAddress: common.HexToAddress(cfg.Contracts.LsdFactoryAddress),
		batchRequestBlocksNumber: cfg.BatchRequestBlocksNumber,
		localSyncedBlockHeight:   localSyncedBlockHeight,
		localStore:               localStore,

		govDeposits:         make(map[string][][]byte),
		validators:          make(map[string]*Validator),
		validatorsByIndex:   make(map[uint64]*Validator),
		nodes:               make(map[common.Address]*Node),
		stakerWithdrawals:   make(map[uint64]*StakerWithdrawal),
		exitElections:       make(map[uint64]*ExitElection),
		cacheEpochToBlockID: cacheEpochToBlockID,
	}

	return s, nil
}

func (s *Service) Start() error {
	chainId, err := s.connection.ChainID()
	if err != nil {
		return err
	}

	s.eth2Config, err = s.connection.Eth2Config()
	if err != nil {
		return err
	}

	switch chainId.Uint64() {
	case 105105: // stratis
		if !bytes.Equal(s.eth2Config.GenesisForkVersion, params.MainnetConfig().GenesisForkVersion) {
			return fmt.Errorf("endpoint network not match")
		}

		domain, err := signing.ComputeDomain(
			params.MainnetConfig().DomainDeposit,
			params.MainnetConfig().GenesisForkVersion,
			params.MainnetConfig().ZeroHash[:],
		)
		if err != nil {
			return err
		}
		s.domain = domain

	case 205205: // auroria
		if !bytes.Equal(s.eth2Config.GenesisForkVersion, params.AuroriaConfig().GenesisForkVersion) {
			return fmt.Errorf("endpoint network not match")
		}

		domain, err := signing.ComputeDomain(
			params.AuroriaConfig().DomainDeposit,
			params.AuroriaConfig().GenesisForkVersion,
			params.AuroriaConfig().ZeroHash[:],
		)
		if err != nil {
			return err
		}
		s.domain = domain
	default:
		return fmt.Errorf("unsupported chainId: %d", chainId.Int64())
	}
	if err != nil {
		return err
	}
	s.chainID = chainId.Uint64()

	s.log.Info("init contracts...")
	err = s.initContract()
	if err != nil {
		return err
	}
	s.log.Debug("init contracts end")

	credentials, err := s.nodeDepositContract.WithdrawCredentials(nil)
	if err != nil {
		return err
	}
	s.withdrawCredentials = credentials

	// get updateBalances epochs
	updateBalancesEpochs, err := s.networkBalancesContract.UpdateBalancesEpochs(nil)
	if err != nil {
		return err
	}
	if updateBalancesEpochs.Uint64() == 0 {
		return fmt.Errorf("updateBalancesEpochs is zero")
	}

	s.submitBalancesDuEpochs = updateBalancesEpochs.Uint64()
	s.distributeWithdrawalsDuEpochs = updateBalancesEpochs.Uint64()
	s.distributePriorityFeeDuEpochs = updateBalancesEpochs.Uint64()
	s.merkleRootDuEpochs = updateBalancesEpochs.Uint64()

	// init commission
	nodeCommissionRate, err := s.networkWithdrawContract.NodeCommissionRate(nil)
	if err != nil {
		return err
	}
	s.nodeCommissionRate = decimal.NewFromBigInt(nodeCommissionRate, 0).Div(decimal.NewFromInt(1e18))
	platformCommissionRate, err := s.networkWithdrawContract.PlatformCommissionRate(nil)
	if err != nil {
		return err
	}
	s.platformCommissionRate = decimal.NewFromBigInt(platformCommissionRate, 0).Div(decimal.NewFromInt(1e18))

	s.log.Infof("nodeCommission rate: %s, platformCommission rate: %s", s.nodeCommissionRate.String(), s.platformCommissionRate.String())

	// init cycle seconds
	cycleSeconds, err := s.networkWithdrawContract.WithdrawCycleSeconds(nil)
	if err != nil {
		return err
	}
	s.cycleSeconds = cycleSeconds.Uint64()

	// init latest block and slot number
	s.latestBlockOfUpdateValidator = s.startAtBlock
	s.latestBlockOfSyncEvents = s.startAtBlock
	if err = s.initLatestBlockOfSyncBlock(); err != nil {
		return err
	}

	block, err := s.connection.Eth1Client().BlockByNumber(context.Background(), big.NewInt(int64(s.latestBlockOfSyncBlock)))
	if err != nil {
		return err
	}
	s.latestSlotOfSyncBlock = utils.SlotAtTimestamp(s.eth2Config, block.Time())
	s.log.Debugf("latestSlotOfSyncBlock: %d, latestBlockOfSyncBlock: %d", s.latestSlotOfSyncBlock, s.latestBlockOfSyncBlock)

	// init abi
	s.networkWithdrawAbi, err = abi.JSON(strings.NewReader(network_withdraw.NetworkWithdrawABI))
	if err != nil {
		return err
	}
	s.networkBalancesAbi, err = abi.JSON(strings.NewReader(network_balances.NetworkBalancesABI))
	if err != nil {
		return err
	}
	s.nodeDepositAbi, err = abi.JSON(strings.NewReader(node_deposit.NodeDepositABI))
	if err != nil {
		return err
	}

	// start services
	s.log.Info("start services...")
	if s.waitFirstNodeStakeEvent {
		s.startSeekFirstNodeStakeEvent()
	} else {
		s.startHandlers()
	}

	return nil
}

func (s *Service) startSeekFirstNodeStakeEvent() {
	log := s.log.WithFields(logrus.Fields{
		"service": "seekingFirstNodeStake",
	})
	utils.SafeGo(func() {
		log.Info("start service")
		defer log.Info("service stopped")
		for {
			select {
			case <-s.stop:
				return
			default:
				found, err := s.seekFirstNodeStakeEvent()
				if found {
					// first node stake event has been found
					log.Info("found first node stake event")
					return
				}
				if err != nil {
					log.WithFields(logrus.Fields{
						"err": err,
					}).Warn("seek first node stake event error")
				}
				utils.Sleep(s.stop, time.Minute*30)
			}
		}
	})
}
func (s *Service) seekFirstNodeStakeEvent() (bool, error) {
	latestBlock, err := s.connection.Eth1LatestBlock()
	if err != nil {
		return false, err
	}
	start := s.latestBlockOfSyncBlock
	end := latestBlock

	for subStart := start; subStart <= end; subStart += fetchEventBlockLimit {
		subEnd := subStart + fetchEventBlockLimit
		if end < subEnd {
			subEnd = end
		}

		iter, err := retry.DoWithData(func() (*node_deposit.NodeDepositDepositedIterator, error) {
			return s.nodeDepositContract.FilterDeposited(&bind.FilterOpts{
				Start:   subStart,
				End:     &subEnd,
				Context: context.Background(),
			})
		}, retry.Delay(time.Second*2), retry.Attempts(5))
		if err != nil {
			return false, err
		}

		hasEvent := iter.Next()
		iter.Close()
		if hasEvent {
			// found the first node stake event
			s.waitFirstNodeStakeEvent = false
			s.startAtBlock = utils.Max(iter.Event.Raw.BlockNumber-2, s.startAtBlock)
			s.latestBlockOfSyncBlock = s.startAtBlock
			s.latestBlockOfUpdateValidator = s.latestBlockOfSyncBlock
			s.latestBlockOfSyncEvents = s.latestBlockOfSyncBlock

			block, err := s.connection.Eth1Client().BlockByNumber(context.Background(), big.NewInt(int64(s.latestBlockOfSyncBlock)))
			if err != nil {
				return false, err
			}
			s.latestSlotOfSyncBlock = utils.SlotAtTimestamp(s.eth2Config, block.Time())

			s.startHandlers()
			return true, nil
		}
	}

	if err = s.localStore.Update(local_store.Info{
		SyncedHeight: end,
		Address:      s.lsdTokenAddress.Hex(),
	}); err != nil {
		return false, err
	}
	s.latestBlockOfSyncBlock = end
	return false, nil
}

func (s *Service) startHandlers() {
	s.startServiceOnce.Do(func() {
		s.minExecutionBlockHeight = s.startAtBlock
		s.log.WithFields(logrus.Fields{
			"latestBlockOfSyncBlock": s.latestBlockOfSyncBlock,
		}).Info("start voting handlers")

		s.startGroupHandlers(func() time.Duration {
			return time.Duration(s.eth2Config.SecondsPerSlot) * time.Second
		}, s.syncBlocks)
		s.startGroupHandlers(func() time.Duration {
			return time.Duration(s.eth2Config.SecondsPerSlot) * time.Second
		}, s.syncEvents, s.updateValidatorsFromNetwork, s.voteWithdrawCredentials)
		s.startGroupHandlers(func() time.Duration {
			slotDur := time.Duration(s.eth2Config.SecondsPerSlot) * time.Second
			epochDur := time.Duration(s.eth2Config.SlotsPerEpoch) * slotDur
			beaconHead, err := s.connection.BeaconHead()
			if err != nil {
				return 6 * slotDur
			}
			// use 2 advance epoch to calc sleep duration
			epoch := beaconHead.Epoch + 2
			targetEpoch := (epoch / s.submitBalancesDuEpochs) * s.submitBalancesDuEpochs
			distance := epoch - targetEpoch
			if distance < s.submitBalancesDuEpochs/10 {
				return slotDur
			} else if distance < s.submitBalancesDuEpochs/2 {
				return epochDur
			}
			return 2 * epochDur
		},
			s.updateValidatorsFromBeacon, s.submitBalances, s.distributeWithdrawals,
			s.distributePriorityFee, s.setMerkleRoot, s.notifyValidatorExit, s.pruneBlocks)
	})
}

func (s *Service) Stop() {
	close(s.stop)
}

func (s *Service) initContract() error {
	var err error

	s.lsdNetworkFactoryContract, err = lsd_network_factory.NewLsdNetworkFactory(s.lsdNetworkFactoryAddress, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	networkContracts, err := s.lsdNetworkFactoryContract.NetworkContractsOfLsdToken(nil, s.lsdTokenAddress)
	if err != nil {
		return err
	}
	s.log.Infof("networkContracts: %+v", networkContracts)

	ethDepositAddress, err := s.lsdNetworkFactoryContract.EthDepositAddress(nil)
	if err != nil {
		return err
	}

	s.govDepositContract, err = deposit_contract.NewDepositContract(ethDepositAddress, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	s.nodeDepositContract, err = node_deposit.NewCustomNodeDeposit(networkContracts.NodeDeposit, s.connection.Eth1Client(), s.connection.MultiCaller())
	if err != nil {
		return err
	}

	s.userDepositContract, err = user_deposit.NewUserDeposit(networkContracts.UserDeposit, s.connection.Eth1Client())
	if err != nil {
		return err
	}
	s.networkWithdrawContract, err = network_withdraw.NewNetworkWithdraw(networkContracts.NetworkWithdraw, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	s.networkProposalContract, err = network_proposal.NewNetworkProposal(networkContracts.NetworkProposal, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	s.networkBalancesContract, err = network_balances.NewNetworkBalances(networkContracts.NetworkBalances, s.connection.Eth1Client())
	if err != nil {
		return err
	}
	s.lsdTokenContract, err = erc20.NewErc20(s.lsdTokenAddress, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	s.feePoolContract, err = fee_pool.NewFeePool(networkContracts.FeePool, s.connection.Eth1Client())
	if err != nil {
		return err
	}

	s.networkWithdrawAddress = networkContracts.NetworkWithdraw
	s.networkBalancesAddress = networkContracts.NetworkBalances
	s.nodeDepositAddress = networkContracts.NodeDeposit

	s.startAtBlock = networkContracts.Block.Uint64()

	s.feePoolAddress = networkContracts.FeePool

	return nil
}

func (s *Service) initLatestBlockOfSyncBlock() error {
	s.latestBlockOfSyncBlock = math.MaxUint64
	checkAndUpdateLatestBlockOfSyncBlock := func(block uint64) {
		s.log.Debugf("checkAndUpdateLatestBlockOfSyncBlock block: %d", block)
		if block < s.latestBlockOfSyncBlock {
			s.latestBlockOfSyncBlock = block
		}
	}

	latestDistributePriorityFeeHeight, err := s.networkWithdrawContract.LatestDistributePriorityFeeHeight(nil)
	if err != nil {
		return err
	}
	checkAndUpdateLatestBlockOfSyncBlock(latestDistributePriorityFeeHeight.Uint64())

	merkleRootEpoch, err := s.networkWithdrawContract.LatestMerkleRootEpoch(nil)
	if err != nil {
		return err
	}
	if merkleRootEpoch.Uint64() > 0 {
		epochBlockNumber, err := s.getEpochStartBlocknumberWithCheck(merkleRootEpoch.Uint64())
		if err != nil {
			return err
		}
		checkAndUpdateLatestBlockOfSyncBlock(epochBlockNumber)
	} else {
		checkAndUpdateLatestBlockOfSyncBlock(0)
	}

	// latest block should less than LatestDistributeWithdrawalsHeight at cycle snapshot
	_, targetTimestamp, err := s.currentCycleAndStartTimestamp()
	if err != nil {
		return fmt.Errorf("currentCycleAndStartTimestamp failed: %w", err)
	}
	targetEpoch := utils.EpochAtTimestamp(s.eth2Config, uint64(targetTimestamp))
	targetBlockNumber, err := s.getEpochStartBlocknumberWithCheck(targetEpoch)
	if err != nil {
		return err
	}
	targetCall := s.connection.CallOpts(big.NewInt(int64(targetBlockNumber)))
	latestDistributeWithdrawalHeight, err := s.networkWithdrawContract.LatestDistributeWithdrawalsHeight(targetCall)
	if err != nil {
		return err
	}
	checkAndUpdateLatestBlockOfSyncBlock(latestDistributeWithdrawalHeight.Uint64())

	// should greater network create block
	if s.latestBlockOfSyncBlock < s.startAtBlock {
		s.latestBlockOfSyncBlock = s.startAtBlock
		s.waitFirstNodeStakeEvent = true
	}
	// should be greater than local synced block height
	if s.latestBlockOfSyncBlock < s.localSyncedBlockHeight {
		s.latestBlockOfSyncBlock = s.localSyncedBlockHeight
		s.waitFirstNodeStakeEvent = true
	}

	return nil
}

func (s *Service) startGroupHandlers(sleepIntervalFn func() time.Duration, handlerFns ...func() error) {
	if len(handlerFns) == 0 {
		panic("handlers can not be empty")
	}

	handlers := make([]Handler, 0, len(handlerFns))
	for _, handler := range handlerFns {

		funcNameRaw := runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()

		splits := strings.Split(funcNameRaw, "/")
		funcName := splits[len(splits)-1]
		funcName = strings.TrimPrefix(funcName, "service.(*Service).")
		funcName = strings.TrimSuffix(funcName, "-fm")

		handlers = append(handlers, Handler{
			method: handler,
			name:   funcName,
		})
	}

	log := s.log
	utils.SafeGo(func() {
		log.Info("start service")
		retry := 0
		var retryLog *logrus.Entry

	Out:
		for {
			if retry > utils.RetryLimit {
				if retryLog != nil {
					retryLog.Errorf("shutting down for too many attempts failed, check your RPC status first")
				}
				utils.ShutdownRequestChannel <- struct{}{}
				return
			}

			select {
			case <-s.stop:
				log.Info("service stopped")
				return
			default:

				for _, handler := range handlers {
					funcName := handler.name
					log := log.WithField("handler", funcName)
					log.Debugf("handler begin")

					err := handler.method()
					if err != nil {
						if errors.Is(err, ErrHandlerExit) {
							log.Error(err.Error())
							utils.ShutdownRequestChannel <- struct{}{}
							return
						}

						retryIn := sleepIntervalFn()
						var gasErr *connection.GasPriceError
						if errors.As(err, &gasErr) {
							log.WithField("retry_in", retryIn).Error(gasErr.Error())
							time.Sleep(retryIn)
							continue Out
						}

						retry++
						retryLog = log.WithFields(logrus.Fields{
							"retry_times": retry,
							"err":         err,
						})
						log := retryLog.WithField("retry_in", retryIn)
						if retry < 50 {
							log.Debugf("failed waiting retry")
						} else {
							log.Warnf("failed waiting retry")
						}
						time.Sleep(retryIn)
						continue Out
					}
					log.Debugf("handler end")
				}

				retry = 0
				retryLog = nil
			}

			time.Sleep(sleepIntervalFn())
		}
	})
}

func (s *Service) GetValidatorDepositedListBeforeBlock(block uint64) []*Validator {
	selectedValidator := make([]*Validator, 0)
	for _, v := range s.validators {
		if v.DepositBlock <= block {
			selectedValidator = append(selectedValidator, v)
		}
	}
	return selectedValidator
}

func (s *Service) getBeaconBlock(eth1BlockNumber uint64) (*CachedBeaconBlock, error) {
	block, exist := s.manager.cachedBeaconBlockByExecBlockHeight.Load(eth1BlockNumber)
	if !exist {
		return nil, fmt.Errorf("getBeaconBlockByEth1BlockNumber %d error: not in cache", eth1BlockNumber)
	}
	return block, nil
}

func (s *Service) getValidatorByIndex(valIndex uint64) (*Validator, bool) {
	s.validatorsByIndexMutex.RLock()
	defer s.validatorsByIndexMutex.RUnlock()

	v, exist := s.validatorsByIndex[valIndex]
	return v, exist
}

func (s *Service) notExitElectionListBefore(targetEpoch, willDealCycle uint64) []*ExitElection {
	els := make([]*ExitElection, 0)
	for cycle, e := range s.exitElections {
		if cycle >= willDealCycle {
			continue
		}
		for _, valIndex := range e.ValidatorIndexList {
			val, exist := s.getValidatorByIndex(valIndex)
			if exist {

				if val.ExitEpoch == 0 {
					els = append(els, e)
					break
				} else {
					if val.ExitEpoch > targetEpoch {
						els = append(els, e)
					}
				}
			}
		}
	}

	sort.SliceStable(els, func(i, j int) bool {
		return els[i].WithdrawCycle < els[j].WithdrawCycle
	})

	return els
}
