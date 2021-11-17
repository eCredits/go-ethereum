package clique

import (
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// SuperTally is the tally for voting on supervalidators
// It only has one vote type as only supervalidators vote.
type SuperTally struct {
	Authorize bool `json:"authorize"` // Whether the vote is about authorizing or kicking someone
	Votes     int  `json:"votes"`     // Number of votes until now wanting to pass the proposal
}

// check if signer is currently a supersigner
func (s *Snapshot) isSuper(signer common.Address) bool {
	_, ok := s.SuperSigners[signer]
	return ok
}

// check if pallas is active at the given blockNumber
func (s *Snapshot) isPallasActive(blockNumber uint64) bool {
	if s.config.Pallas == nil {
		return false
	}
	if s.config.Pallas.Block == nil {
		return false
	}
	return blockNumber >= s.config.Pallas.Block.Uint64()
}

// validVoteSuper returns whether it makes sense to cast the specified super vote
// super votes can only be cast for addresses which are validators already
func (s *Snapshot) validVoteSuper(address common.Address, authorize bool) bool {
	_, regularsigner := s.Signers[address]
	_, signer := s.SuperSigners[address]
	return (signer && !authorize) || (!signer && regularsigner && authorize)
}

// cast adds a new vote into the tally.
func (s *Snapshot) castSuper(address common.Address, authorize bool) bool {
	// Ensure the vote is meaningful
	if !s.validVoteSuper(address, authorize) {
		return false
	}
	// Cast the vote into an existing or new tally
	if old, ok := s.SuperTally[address]; ok {
		old.Votes++
		s.SuperTally[address] = old
	} else {
		new := SuperTally{Authorize: authorize, Votes: 1}
		s.SuperTally[address] = new
	}
	return true
}

// uncast removes a previously cast vote from the tally.
func (s *Snapshot) uncastSuper(address common.Address, authorize bool) bool {
	// If there's no tally, it's a dangling vote, just drop
	tally, ok := s.SuperTally[address]
	if !ok {
		return false
	}
	// Ensure we only revert counted votes
	if tally.Authorize != authorize {
		return false
	}
	// Otherwise revert the vote
	if tally.Votes > 1 {
		tally.Votes--
		s.SuperTally[address] = tally
	} else {
		delete(s.SuperTally, address)
	}
	return true
}

func (s *Snapshot) hasPassedSuper(tally *SuperTally, blockNumber uint64) bool {
	if tally.Votes > len(s.SuperSigners)*3/4 {
		return true
	}
	return false
}

// if pallas is active, check for overrides
func (s *Snapshot) applyPallasOverride(header *types.Header) {
	nextNumber := header.Number.Uint64() + 1
	if s.isPallasActive(nextNumber - 1) {
		// if this is the last block of an epoch, update the signer set now as votes will be cleared at the beginning of the next block
		// this means an ovveride cannot occur on the first pallas block
		if nextNumber%s.config.Epoch == 0 {
			if nextSigners, ok := s.config.Pallas.Validators[nextNumber]; ok {
				newSigners := make(map[common.Address]struct{})
				for _, signer := range nextSigners {
					newSigners[signer.Address] = struct{}{}
				}

				for oldSigner := range s.Signers {
					if _, ok := newSigners[oldSigner]; !ok {
						log.Info("Removing signer", "address", oldSigner.Hex(), "number", nextNumber, "hash", header.Hash)
						delete(s.Signers, oldSigner)
						if _, isSuperSigner := s.SuperSigners[oldSigner]; isSuperSigner {
							log.Info("Removing super signer", "address", oldSigner.Hex(), "number", nextNumber, "hash", header.Hash)
							delete(s.SuperSigners, oldSigner)
						}
					}
				}

				for _, newSigner := range nextSigners {
					if _, ok := s.Signers[newSigner.Address]; !ok {
						log.Info("Adding signer", "address", newSigner.Address.Hex(), "number", nextNumber, "hash", header.Hash)
						s.Signers[newSigner.Address] = struct{}{}
						if newSigner.Super {
							log.Info("Adding super signer", "address", newSigner.Address.Hex(), "number", nextNumber, "hash", header.Hash)
							s.SuperSigners[newSigner.Address] = struct{}{}
						}
					}

					if newSigner.Super {
						s.SuperSigners[newSigner.Address] = struct{}{}
					}
				}

				// reset recents to ensure liveness in case of a smaller set
				// this means a signer can create two blocks in a row on an override
				s.Recents = make(map[uint64]common.Address)
			}
		}
	}
}

func (s *Snapshot) superSigners() []common.Address {
	sigs := make([]common.Address, 0, len(s.SuperSigners))
	for sig := range s.SuperSigners {
		sigs = append(sigs, sig)
	}
	sort.Sort(signersAscending(sigs))
	return sigs
}

func (s *Snapshot) lastSlot(slot uint64) uint64 {
	signer := s.signers()[slot]
	for bn, recent := range s.Recents {
		if recent == signer {
			return bn
		}
	}
	return 0
}

func (s *Snapshot) offset(signer common.Address) int64 {
	signers := s.signers()
	for i, _signer := range signers {
		if _signer == signer {
			return int64(i)
		}
	}
	return int64(len(signers))
}

func (snap *Snapshot) computeDelay(signer common.Address, number uint64) (delay uint64, wiggle bool) {
	signerCount := uint64(len(snap.signers()))
	offset := snap.offset(signer)

	primarySigner := number % signerCount
	secondarySigner := (number%signerCount - 1 + signerCount/2) % signerCount

	primaryLastSlot := snap.lastSlot(primarySigner)

	if primaryLastSlot == 0 {
		// primary is not blocked
		secondaryLastSlot := snap.lastSlot(secondarySigner)
		if secondaryLastSlot != 0 {
			// secondary is blocked because it replaced someone
			replaced := secondaryLastSlot % signerCount
			if replaced == uint64(offset) {
				// the replaced one should jump in
				return 1, false
			}
			// otherwise try later
			return 2, true
		} else if secondarySigner == uint64(offset) {
			// if we are the secondary signer do NOT increase delay
			return 1, false
		}
		// otherwise try later
		return 2, true
	} else {
		// primary is blocked because it replaced someone
		replaced := primaryLastSlot % signerCount
		if snap.lastSlot(replaced) != 0 {
			// if the previously replaced on is still blocked, try the secondary first
			if secondarySigner == uint64(offset) {
				// if we are the secondary signer do NOT increase delay
				return 0, false
			}
			// otherwise try later
			return 2, true
		} else {
			if replaced == uint64(offset) {
				// we are the replaced one and free
				return 0, false
			} else if secondarySigner == uint64(offset) {
				// secondary tries a bit later
				return 1, false
			}
			// otherwise try later
			return 2, true
		}
	}
}
