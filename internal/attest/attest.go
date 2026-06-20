// Package attest parses and verifies attestation CloudEvents: EOA and
// ERC-1271 signature verification, delegation (payload Source overriding the
// JWT holder), tombstone handling, and the dimo.raw.* / dimo.document.* type
// whitelist. It is a pure-Go port of the DIS cloudeventconvert attestation
// path with the Benthos message plumbing removed.
package attest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/web3"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"
)

// MaxTombstoneReasonBytes caps the length of the reason field in a tombstone
// data payload. Tombstones are tiny by design and live alongside attestations
// in the lake; bounding reason keeps row size predictable and limits abuse.
const MaxTombstoneReasonBytes = 512

// ErrValidation marks 400-class errors caused by invalid attestation payloads
// or signatures. It is the same sentinel used by the convert package, so
// errors.Is works across both.
var ErrValidation = convert.ErrValidation

var erc1271magicValue = [4]byte{0x16, 0x26, 0xba, 0x7e}

// Attestation is a verified attestation CloudEvent.
type Attestation struct {
	cloudevent.RawEvent
	// VoidsID is set for dimo.tombstone events: the id of the attestation
	// being voided. It is empty for every other attestation type.
	VoidsID string
}

// Verifier parses attestation payloads and verifies their signatures. EOA
// signatures are checked locally; ERC-1271 signatures are checked against the
// source contract via the configured Ethereum backend.
type Verifier struct {
	logger  zerolog.Logger
	backend bind.ContractBackend
}

// NewVerifier dials rpcURL and returns a Verifier backed by that client.
func NewVerifier(rpcURL string, logger zerolog.Logger) (*Verifier, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rpc url %s: %w", rpcURL, err)
	}
	return NewVerifierWithBackend(client, logger), nil
}

// NewVerifierWithBackend returns a Verifier using the given contract backend
// for ERC-1271 signature checks.
func NewVerifierWithBackend(backend bind.ContractBackend, logger zerolog.Logger) *Verifier {
	return &Verifier{
		logger:  logger,
		backend: backend,
	}
}

// Parse parses, validates, and signature-verifies an attestation payload.
// jwtAddress is the Ethereum address of the JWT holder; if the payload
// includes its own source, that source is used instead (delegation support).
// For dimo.tombstone events the returned Attestation carries the voided
// attestation id in VoidsID.
func (v *Verifier) Parse(ctx context.Context, jwtAddress common.Address, body []byte) (*Attestation, error) {
	event, err := parseAndValidateAttestation(body, jwtAddress.Hex())
	if err != nil {
		return nil, fmt.Errorf("failed to process attestation: %w", err)
	}

	validSignature, err := v.verifySignature(ctx, event, common.HexToAddress(event.Source))
	if err != nil {
		return nil, fmt.Errorf("failed to check message signature: %w", err)
	}

	if !validSignature {
		return nil, fmt.Errorf("%w: message signature invalid", ErrValidation)
	}

	attestation := &Attestation{RawEvent: *event}
	if event.Type == cloudevent.TypeAttestationTombstone {
		voidsID, err := parseTombstoneData(event.Data)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid tombstone payload: %w", ErrValidation, err)
		}
		attestation.VoidsID = voidsID
	}

	return attestation, nil
}

// tombstoneData is the expected shape of a dimo.tombstone event's data payload.
type tombstoneData struct {
	VoidsID string `json:"voidsId"`
	Reason  string `json:"reason,omitempty"`
}

// parseTombstoneData parses and validates a tombstone's data payload.
// It returns the target attestation id (the value of voidsId).
func parseTombstoneData(data json.RawMessage) (string, error) {
	if len(data) == 0 {
		return "", errors.New("data payload is required for tombstones")
	}
	var td tombstoneData
	if err := json.Unmarshal(data, &td); err != nil {
		return "", fmt.Errorf("data payload is not a tombstone object: %w", err)
	}
	if td.VoidsID == "" {
		return "", errors.New("voidsId is required and must be non-empty")
	}
	if !convert.ValidIdentifier(td.VoidsID) {
		return "", fmt.Errorf("invalid voidsId: %s", td.VoidsID)
	}
	if len(td.Reason) > MaxTombstoneReasonBytes {
		return "", fmt.Errorf("reason length %d exceeds max %d", len(td.Reason), MaxTombstoneReasonBytes)
	}
	return td.VoidsID, nil
}

// parseAndValidateAttestation unmarshals an attestation cloud event and
// validates it. It rewrites Subject and Source on the returned event so
// any contract or account address is in EIP-55 checksum form: a
// lowercased / mixed-case `did:erc721:` or `did:ethr:` Subject is
// re-serialized via DID.String(), and Source is normalized via
// common.HexToAddress(...).Hex() for consistent downstream storage.
func parseAndValidateAttestation(msgBytes []byte, source string) (*cloudevent.RawEvent, error) {
	var event cloudevent.RawEvent
	if err := json.Unmarshal(msgBytes, &event); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal attestation cloud event: %w", ErrValidation, err)
	}

	if convert.IsFutureTimestamp(event.Time) {
		return nil, fmt.Errorf("%w: event timestamp %v exceeds valid range", ErrValidation, event.Time)
	}

	if did, err := cloudevent.DecodeERC721DID(event.Subject); err == nil {
		event.Subject = did.String()
	} else if did, err := cloudevent.DecodeEthrDID(event.Subject); err == nil {
		event.Subject = did.String()
	} else {
		return nil, fmt.Errorf("%w: invalid attestation subject format: %w", ErrValidation, err)
	}

	// If the payload includes a source, use it (delegation support);
	// otherwise fall back to the JWT holder's address.
	resolvedSource := source
	if event.Source != "" {
		resolvedSource = event.Source
	}
	if !common.IsHexAddress(resolvedSource) {
		return nil, fmt.Errorf("%w: invalid source address: %s", ErrValidation, resolvedSource)
	}
	// Normalize to EIP-55 checksummed form for consistent storage.
	resolvedSource = common.HexToAddress(resolvedSource).Hex()

	if err := convert.ValidateHeadersAndSetDefaults(&event.CloudEventHeader, resolvedSource, ksuid.New().String(), event.DataBase64 != ""); err != nil {
		return nil, fmt.Errorf("failed to validate headers: %w", err)
	}

	if event.Type == "" {
		event.Type = cloudevent.TypeAttestation
	}
	if !isValidAttestationType(event.Type) {
		return nil, fmt.Errorf("%w: invalid attestation type %q: must be dimo.attestation, dimo.tombstone, dimo.raw.*, or dimo.document.*", ErrValidation, event.Type)
	}
	return &event, nil
}

func isValidAttestationType(t string) bool {
	switch {
	case t == cloudevent.TypeAttestation:
		return true
	case t == cloudevent.TypeAttestationTombstone:
		return true
	case strings.HasPrefix(t, "dimo.raw.") && len(t) > len("dimo.raw."):
		return true
	case strings.HasPrefix(t, "dimo.document.") && len(t) > len("dimo.document."):
		return true
	default:
		return false
	}
}

// verifySignature attempts to verify the signed data.
// first check if the source is the signer
// if the source is not the signer, check whether the signature is from a dev license where the source is the contract addr
func (v *Verifier) verifySignature(ctx context.Context, event *cloudevent.RawEvent, source common.Address) (bool, error) {
	signature := common.FromHex(event.Signature)

	msgHashWithPrfx := accounts.TextHash(event.Data)
	eoaSigner, errEoa := verifyEOASignature(signature, msgHashWithPrfx, source)
	if errEoa != nil || !eoaSigner {
		erc1271Signer, errErc := v.verifyERC1271Signature(ctx, signature, common.BytesToHash(msgHashWithPrfx), source)
		if errErc != nil {
			return false, errors.Join(errEoa, errErc)
		}

		return erc1271Signer, nil
	}

	return true, nil
}

func verifyEOASignature(signature []byte, msgHash []byte, source common.Address) (bool, error) {
	if len(signature) != 65 {
		return false, fmt.Errorf("signature has length %d != 65", len(signature))
	}

	sigCopy := make([]byte, len(signature))
	copy(sigCopy, signature)

	sigCopy[64] -= 27
	if sigCopy[64] != 0 && sigCopy[64] != 1 {
		return false, fmt.Errorf("invalid v byte: %d; accepted values 27 or 28", signature[64])
	}

	pubKey, err := crypto.SigToPub(msgHash, sigCopy)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal public key: %w", err)
	}
	recoveredAddress := crypto.PubkeyToAddress(*pubKey)
	return source == recoveredAddress, nil
}

func (v *Verifier) verifyERC1271Signature(ctx context.Context, signature []byte, msgHash common.Hash, source common.Address) (bool, error) {
	contract, err := web3.NewErc1271(source, v.backend)
	if err != nil {
		return false, fmt.Errorf("failed to connect to address: %s: %w", source, err)
	}

	result, err := contract.IsValidSignature(&bind.CallOpts{Context: ctx}, msgHash, signature)
	if err != nil {
		return false, fmt.Errorf("failed to validate signature with contract: %w", err)
	}

	return result == erc1271magicValue, nil
}
