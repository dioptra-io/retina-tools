package tier1exclusions

import (
	"net/netip"
	"reflect"
	"sort"
	"testing"
)

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("bad test prefix %q: %v", s, err)
	}
	return p
}

func TestOptimize_DropsDefaultRoute(t *testing.T) {
	// Regression test: RIPEstat's announced-prefixes response included a "0.0.0.0/0"
	// default route in practice, which must be dropped, not treated as a real
	// tier-1 aggregate (seen live in the Aug 6 run logs).
	res := Optimize([]string{"0.0.0.0/0", "8.0.0.0/12"}, true)
	if len(res.TooBroad) != 1 || res.TooBroad[0] != "0.0.0.0/0" {
		t.Errorf("expected 0.0.0.0/0 flagged as too-broad, got TooBroad=%v", res.TooBroad)
	}
	if len(res.Collapsed) != 1 || res.Collapsed[0].String() != "8.0.0.0/12" {
		t.Errorf("expected only 8.0.0.0/12 to survive, got %v", res.Collapsed)
	}
}

func TestOptimize_MixedAFIDoesNotPanic(t *testing.T) {
	// Regression test: the original prototype crashed with
	// "TypeError: ... and ... are not of the same version" the first time a mixed
	// v4/v6 list reached collapse_addresses(). Optimize must filter by AFI up front,
	// with no possibility of a v4/v6 comparison ever happening.
	raw := []string{"8.0.0.0/12", "2620:12c:3000::/44", "12.0.0.0/8", "2a06:6541:2002::/48"}

	v4 := Optimize(raw, true)
	if len(v4.Collapsed) != 2 {
		t.Errorf("expected 2 v4 prefixes collapsed, got %d: %v", len(v4.Collapsed), v4.Collapsed)
	}
	for _, p := range v4.Collapsed {
		if !p.Addr().Is4() {
			t.Errorf("Optimize(afiIs4=true) returned a non-v4 prefix: %s", p)
		}
	}

	v6 := Optimize(raw, false)
	if len(v6.Collapsed) != 2 {
		t.Errorf("expected 2 v6 prefixes collapsed, got %d: %v", len(v6.Collapsed), v6.Collapsed)
	}
	for _, p := range v6.Collapsed {
		if p.Addr().Is4() {
			t.Errorf("Optimize(afiIs4=false) returned a v4 prefix: %s", p)
		}
	}
}

func TestOptimize_InvalidPrefixesAreReportedNotSilentlyDropped(t *testing.T) {
	res := Optimize([]string{"not-a-prefix", "8.0.0.0/12", "999.999.999.999/24"}, true)
	if len(res.Invalid) != 2 {
		t.Errorf("expected 2 invalid entries reported, got %d: %v", len(res.Invalid), res.Invalid)
	}
	if len(res.Collapsed) != 1 {
		t.Errorf("expected 1 valid prefix to survive, got %v", res.Collapsed)
	}
}

func TestIsSiblingPair(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"real siblings", "10.0.0.0/25", "10.0.0.128/25", true},
		{"siblings, reversed order", "10.0.0.128/25", "10.0.0.0/25", true},
		{"same prefix twice", "10.0.0.0/25", "10.0.0.0/25", false},
		{"different lengths", "10.0.0.0/25", "10.0.0.128/24", false},
		{"same length, different parent", "10.0.0.0/25", "10.0.1.128/25", false},
		{"not adjacent, same parent length", "10.0.0.0/26", "10.0.0.192/26", false},
	}
	for _, c := range cases {
		a, b := mustPrefix(t, c.a), mustPrefix(t, c.b)
		if got := isSiblingPair(a, b); got != c.want {
			t.Errorf("%s: isSiblingPair(%s, %s) = %v, want %v", c.name, c.a, c.b, got, c.want)
		}
	}
}

func TestOptimize_CollapsesSubsetsAndSiblings(t *testing.T) {
	res := Optimize([]string{
		"8.0.0.0/9",     // aggregate
		"8.10.120.0/24", // subset of the /9 — should be dropped by collapse
		"9.0.0.0/25",    // sibling pair with 9.0.0.128/25 -> should merge to /24
		"9.0.0.128/25",
	}, true)

	got := prefixStrings(res.Collapsed)
	sort.Strings(got)
	want := []string{"8.0.0.0/9", "9.0.0.0/24"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("collapse mismatch:\n got:  %v\n want: %v", got, want)
	}
}

func TestQueryGroup_String(t *testing.T) {
	g := QueryGroup{
		Supernet: mustPrefix(t, "8.0.0.0/12"),
		Prefixes: []netip.Prefix{mustPrefix(t, "8.0.0.0/16"), mustPrefix(t, "8.1.0.0/16")},
	}
	want := "8.0.0.0/12 (2 prefixes)"
	if got := g.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestBuildQueryGroups_GroupsUnderPrefixLength(t *testing.T) {
	prefixes := []netip.Prefix{
		mustPrefix(t, "8.10.0.0/16"),
		mustPrefix(t, "8.11.0.0/16"),
		mustPrefix(t, "8.0.0.0/9"), // already broader than /12 -> its own group
	}
	groups := BuildQueryGroups(prefixes, 12)

	if len(groups) != 2 {
		t.Fatalf("expected 2 query groups, got %d: %+v", len(groups), groups)
	}
	// the /9 should be its own group (can't widen past its own bits)
	foundOwnGroup := false
	for _, g := range groups {
		if g.Supernet.String() == "8.0.0.0/9" {
			foundOwnGroup = true
			if len(g.Prefixes) != 1 {
				t.Errorf("expected the /9 to group only itself, got %d prefixes", len(g.Prefixes))
			}
		}
	}
	if !foundOwnGroup {
		t.Errorf("expected a group exactly at 8.0.0.0/9, groups: %+v", groups)
	}
}

func TestAssignExclusions_OwnSpaceKeptCustomerExcluded(t *testing.T) {
	parent := mustPrefix(t, "8.0.0.0/12")
	moreSpecifics := []MoreSpecific{
		{Prefix: mustPrefix(t, "8.0.1.0/24"), Origins: set("3356")},          // tier-1's own -> keep
		{Prefix: mustPrefix(t, "8.0.2.0/24"), Origins: set("64500")},         // customer -> exclude
		{Prefix: mustPrefix(t, "8.0.3.0/24"), Origins: set("3356", "64500")}, // MOAS, disagreement -> exclude (conservative)
	}

	result := AssignExclusions(moreSpecifics, []netip.Prefix{parent}, "3356")
	excl := result["8.0.0.0/12"]
	want := []string{"8.0.2.0/24", "8.0.3.0/24"}
	if !reflect.DeepEqual(excl, want) {
		t.Errorf("exclusions mismatch:\n got:  %v\n want: %v", excl, want)
	}
}

func TestAssignExclusions_MapsToCorrectParentAmongMultiple(t *testing.T) {
	// Regression-style test for the bisect/LPM logic: with several non-overlapping
	// parents under one batched cover, each discovered prefix must land under the
	// ONE parent that actually contains it, not an adjacent one.
	parents := []netip.Prefix{
		mustPrefix(t, "8.0.0.0/16"),
		mustPrefix(t, "8.2.0.0/16"),
		mustPrefix(t, "8.4.0.0/16"),
	}
	moreSpecifics := []MoreSpecific{
		{Prefix: mustPrefix(t, "8.2.5.0/24"), Origins: set("999")},
		{Prefix: mustPrefix(t, "8.0.9.0/24"), Origins: set("999")},
		{Prefix: mustPrefix(t, "8.4.1.0/24"), Origins: set("999")},
		{Prefix: mustPrefix(t, "8.3.0.0/24"), Origins: set("999")}, // NOT inside any parent -> ignored
	}

	result := AssignExclusions(moreSpecifics, parents, "3356")
	if got := result["8.0.0.0/16"]; !reflect.DeepEqual(got, []string{"8.0.9.0/24"}) {
		t.Errorf("8.0.0.0/16: got %v", got)
	}
	if got := result["8.2.0.0/16"]; !reflect.DeepEqual(got, []string{"8.2.5.0/24"}) {
		t.Errorf("8.2.0.0/16: got %v", got)
	}
	if got := result["8.4.0.0/16"]; !reflect.DeepEqual(got, []string{"8.4.1.0/24"}) {
		t.Errorf("8.4.0.0/16: got %v", got)
	}
	for k, v := range result {
		for _, p := range v {
			if p == "8.3.0.0/24" {
				t.Errorf("8.3.0.0/24 should not have been assigned anywhere, found under %s", k)
			}
		}
	}
}

func TestAssignExclusions_UnseenOriginTreatedAsNotTheASN(t *testing.T) {
	// A more-specific with an EMPTY origin set (e.g. malformed AS path) must never be
	// silently treated as "the tier-1's own" — allOriginsAre must return false for
	// an empty set, not vacuously true.
	parent := mustPrefix(t, "8.0.0.0/12")
	moreSpecifics := []MoreSpecific{
		{Prefix: mustPrefix(t, "8.0.1.0/24"), Origins: map[string]struct{}{}},
	}
	result := AssignExclusions(moreSpecifics, []netip.Prefix{parent}, "3356")
	if got := result["8.0.0.0/12"]; !reflect.DeepEqual(got, []string{"8.0.1.0/24"}) {
		t.Errorf("expected empty-origin prefix to be excluded (conservative), got %v", got)
	}
}

func TestAssignExclusions_DuplicateMoreSpecificsDeduped(t *testing.T) {
	// Defensive: the real caller (collectMoreSpecifics) already dedupes by prefix
	// upstream, but AssignExclusions shouldn't produce duplicate output even if a
	// future caller passes the same MoreSpecific twice.
	parent := mustPrefix(t, "8.0.0.0/12")
	moreSpecifics := []MoreSpecific{
		{Prefix: mustPrefix(t, "8.0.1.0/24"), Origins: set("64500")},
		{Prefix: mustPrefix(t, "8.0.1.0/24"), Origins: set("64500")}, // duplicate
	}
	result := AssignExclusions(moreSpecifics, []netip.Prefix{parent}, "3356")
	want := []string{"8.0.1.0/24"}
	if got := result["8.0.0.0/12"]; !reflect.DeepEqual(got, want) {
		t.Errorf("expected duplicates collapsed to one entry, got %v", got)
	}
}

func set(vals ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return m
}

func prefixStrings(ps []netip.Prefix) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.String()
	}
	return out
}
