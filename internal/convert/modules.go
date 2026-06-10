package convert

import (
	"github.com/DIMO-Network/model-garage/pkg/autopi"
	"github.com/DIMO-Network/model-garage/pkg/hashdog"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/ruptela"
	"github.com/ethereum/go-ethereum/common"
)

// Config holds the chain settings used to construct the model-garage
// conversion modules. The values mirror the DIS Benthos processor config
// (chain_id, vehicle_nft_address, aftermarket_nft_address,
// synthetic_nft_address sourced from DIMO_REGISTRY_CHAIN_ID,
// VEHICLE_NFT_ADDRESS, AFTERMARKET_NFT_ADDRESS, SYNTHETIC_NFT_ADDRESS).
type Config struct {
	// ChainID is the chain ID for the Ethereum network.
	ChainID uint64
	// VehicleNFTAddress is the Ethereum address for the vehicles contract.
	VehicleNFTAddress common.Address
	// AftermarketNFTAddress is the Ethereum address for the aftermarket contract.
	AftermarketNFTAddress common.Address
	// SyntheticNFTAddress is the Ethereum address for the synthetic device contract.
	SyntheticNFTAddress common.Address
}

// RegisterModules registers the source-specific CloudEvent conversion modules
// in the model-garage registry. All four registrations must be kept in sync
// with DIS; dropping any of them silently breaks the corresponding oracle.
func RegisterModules(cfg Config) {
	// AutoPi
	autoPiModule := &autopi.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	modules.CloudEventRegistry.Override(modules.AutoPiSource.String(), autoPiModule)

	// Ruptela
	ruptelaModule := &ruptela.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	modules.CloudEventRegistry.Override(modules.RuptelaSource.String(), ruptelaModule)

	// Ruptela Protocol - currently handled by Kaufmann Oracle only
	ruptelaSyntheticModule := &ruptela.Module{
		AftermarketContractAddr: cfg.SyntheticNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	modules.CloudEventRegistry.Override(modules.KaufmannSource.String(), ruptelaSyntheticModule)

	// HashDog
	hashDogModule := &hashdog.Module{
		AftermarketContractAddr: cfg.AftermarketNFTAddress,
		VehicleContractAddr:     cfg.VehicleNFTAddress,
		ChainID:                 cfg.ChainID,
	}
	modules.CloudEventRegistry.Override(modules.HashDogSource.String(), hashDogModule)
}
