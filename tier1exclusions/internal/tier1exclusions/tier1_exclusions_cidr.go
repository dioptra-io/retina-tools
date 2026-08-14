// This file implements the CIDR-level logic for the tier-1 exclusion pipeline:
// deduping/collapsing an ASN's announced prefixes, grouping them into query groups,
// and assigning discovered more-specific prefixes back to their correct parent block.
package tier1exclusions

import (
	"fmt"
	"net/netip"
	"sort"
)

// OptimizeResult holds the outcome of deduping/collapsing a raw prefix list, plus
// what was dropped and why, rather than discarding it silently.
type OptimizeResult struct {
	Collapsed []netip.Prefix
	Invalid   []string // failed to parse as a prefix at all
	TooBroad  []string // parsed fine, but broader than the configured floor
}

// MinPrefixLen guards against absurd entries like a stray "0.0.0.0/0" showing up in
// an ASN's announced-prefix list — /8 (v4) and /16 (v6) are far broader than any real
// tier-1 aggregate should be.
func MinPrefixLen(is4 bool) int {
	if is4 {
		return 8
	}
	return 16
}

// Optimize parses, filters, and collapses a raw list of prefix strings for a single
// address family. Mixed v4/v6 input is fine — filtered by afiIs4 before any
// comparison, so a v4/v6 mismatch can't reach collapse().
func Optimize(raw []string, afiIs4 bool) OptimizeResult {
	var result OptimizeResult
	var kept []netip.Prefix

	for _, s := range raw {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			result.Invalid = append(result.Invalid, s)
			continue
		}
		if p.Addr().Is4() != afiIs4 {
			continue // other address family, not an error
		}
		if p.Bits() < MinPrefixLen(afiIs4) {
			result.TooBroad = append(result.TooBroad, s)
			continue
		}
		kept = append(kept, p.Masked())
	}

	result.Collapsed = collapse(kept)
	return result
}

// collapse merges adjacent/nested prefixes into their minimal covering set.
// Callers must pass a single address family.
func collapse(prefixes []netip.Prefix) []netip.Prefix {
	if len(prefixes) == 0 {
		return nil
	}
	sort.Slice(prefixes, func(i, j int) bool {
		ai, aj := prefixes[i].Addr(), prefixes[j].Addr()
		if ai != aj {
			return ai.Less(aj)
		}
		return prefixes[i].Bits() < prefixes[j].Bits()
	})

	// Only the most recently kept prefix is checked against — not every entry in
	// deduped — because the sort above guarantees that's enough: prefixes are ordered
	// by address, so a broad prefix always comes right before any narrower prefixes it
	// contains (e.g. 8.0.0.0/12 before 8.0.1.0/24). Once a broad prefix is kept, every
	// contained prefix that follows gets caught by checking just that one entry.
	var deduped []netip.Prefix
	for _, p := range prefixes {
		if len(deduped) > 0 && prefixContains(deduped[len(deduped)-1], p) {
			continue
		}
		deduped = append(deduped, p)
	}

	// Merge sibling pairs into their parent (e.g. .0/25 + .128/25 -> /24), repeating
	// until stable.
	changed := true
	for changed {
		changed = false
		var merged []netip.Prefix
		i := 0
		for i < len(deduped) {
			if i+1 < len(deduped) && isSiblingPair(deduped[i], deduped[i+1]) {
				parent := deduped[i].Addr()
				merged = append(merged, netip.PrefixFrom(parent, deduped[i].Bits()-1))
				i += 2
				changed = true
			} else {
				merged = append(merged, deduped[i])
				i++
			}
		}
		deduped = merged
	}
	return deduped
}

// prefixContains reports whether p is contained within container (container must be
// equal or broader).
func prefixContains(container, p netip.Prefix) bool {
	return container.Bits() <= p.Bits() && container.Contains(p.Addr())
}

// isSiblingPair reports whether a and b are the two distinct halves of the same
// parent block (same length, same immediate parent, different addresses).
func isSiblingPair(a, b netip.Prefix) bool {
	if a.Bits() != b.Bits() || a.Bits() == 0 {
		return false
	}
	parentBits := a.Bits() - 1
	parentA := netip.PrefixFrom(a.Addr(), parentBits).Masked()
	parentB := netip.PrefixFrom(b.Addr(), parentBits).Masked()
	return parentA == parentB && a.Addr() != b.Addr()
}

// QueryGroup batches several real announced prefixes under one artificial, coarser
// prefix so they can all be queried together in a single rib() call.
type QueryGroup struct {
	// Supernet is a batching key only — it is NOT a real announced block, just a
	// coarser prefix that covers everything in Prefixes.
	Supernet netip.Prefix
	// Prefixes are the real, collapsed announced prefixes grouped under Supernet.
	// Each one is narrower than or equal to Supernet, never broader.
	Prefixes []netip.Prefix
}

// BuildQueryGroups groups prefixes into query groups of at most groupPrefixLen
// specificity. A prefix already broader than groupPrefixLen becomes its own group
// (its Supernet equals itself — can't widen further).
func BuildQueryGroups(prefixes []netip.Prefix, groupPrefixLen int) []QueryGroup {
	buckets := map[netip.Prefix][]netip.Prefix{}

	for _, p := range prefixes {
		bits := groupPrefixLen
		if p.Bits() < bits {
			bits = p.Bits()
		}
		supernet, err := p.Addr().Prefix(bits)
		if err != nil {
			continue
		}
		buckets[supernet] = append(buckets[supernet], p)
	}

	groups := make([]QueryGroup, 0, len(buckets))
	for supernet, prefixes := range buckets {
		groups = append(groups, QueryGroup{Supernet: supernet, Prefixes: prefixes})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Supernet.Addr().Less(groups[j].Supernet.Addr())
	})
	return groups
}

// MoreSpecific is one discovered prefix nested inside a queried group, with every
// distinct origin ASN observed for it (a MOAS prefix can have >1).
type MoreSpecific struct {
	Prefix  netip.Prefix
	Origins map[string]struct{}
}

// AssignExclusions maps each discovered prefix to its parent block via longest-prefix
// match, and excludes it unless every observed origin matches targetASN — conservative
// MOAS handling: a prefix is only treated as tier-1's own if ALL observed origins
// agree it belongs to targetASN.
func AssignExclusions(moreSpecifics []MoreSpecific, parents []netip.Prefix, targetASN string) map[string][]string {
	sortedParents := append([]netip.Prefix(nil), parents...)
	sort.Slice(sortedParents, func(i, j int) bool {
		return sortedParents[i].Addr().Less(sortedParents[j].Addr())
	})

	result := make(map[string][]string, len(sortedParents))
	for _, p := range sortedParents {
		result[p.String()] = nil
	}

	for _, ms := range moreSpecifics {
		if allOriginsAre(ms.Origins, targetASN) {
			continue
		}
		parent, ok := findParent(sortedParents, ms.Prefix)
		if !ok {
			continue
		}
		key := parent.String()
		result[key] = append(result[key], ms.Prefix.String())
	}

	for k := range result {
		sort.Strings(result[k])
		result[k] = dedupSorted(result[k])
	}
	return result
}

// dedupSorted removes adjacent duplicates from an already-sorted slice. Defensive:
// the real caller (collectMoreSpecifics) already dedupes by prefix upstream, so this
// guards against a future caller passing the same MoreSpecific twice, not a case
// that occurs in the current pipeline.
func dedupSorted(s []string) []string {
	if len(s) <= 1 {
		return s
	}
	out := s[:1]
	for _, v := range s[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

func allOriginsAre(origins map[string]struct{}, asn string) bool {
	if len(origins) == 0 {
		return false
	}
	for o := range origins {
		if o != asn {
			return false
		}
	}
	return true
}

// findParent does a binary-search longest-prefix-match.
// Precondition: sortedParents must be sorted by address and non-overlapping —
// guaranteed for one ASN's collapsed prefix list, not enforced here.
func findParent(sortedParents []netip.Prefix, p netip.Prefix) (netip.Prefix, bool) {
	i := sort.Search(len(sortedParents), func(i int) bool {
		return sortedParents[i].Addr().Compare(p.Addr()) > 0
	}) - 1
	if i < 0 {
		return netip.Prefix{}, false
	}
	if prefixContains(sortedParents[i], p) {
		return sortedParents[i], true
	}
	return netip.Prefix{}, false
}

func (g QueryGroup) String() string {
	return fmt.Sprintf("%s (%d prefixes)", g.Supernet, len(g.Prefixes))
}
