package tui

import (
	"sort"
	"strings"

	"github.com/sonirico/readpanda/pkg/rp"
)

// topicTree groups topics by their dot-separated naming convention so the
// topics view can render them collapsibly (e.g. chesscom → stats → v1 →
// <topic>). Branches aggregate their descendants' message/byte counts so the
// user can see hotspots without expanding every node.
type topicTree struct {
	root     *topicNode
	expanded map[string]bool // keyed by fullPath of branch nodes
}

// topicNode is a single position in the tree. When isLeaf, info carries the
// underlying rp.TopicInfo; otherwise children hold the subtree.
type topicNode struct {
	name     string // segment (e.g. "v1") for branches, full topic name for leaves
	fullPath string // dot-joined path from root
	depth    int
	isLeaf   bool

	info rp.TopicInfo // valid only when isLeaf

	children    map[string]*topicNode
	childKeys   []string // alphabetised keys into children for stable iteration
	leafCount   int      // total leaf descendants (own subtree)
	messagesSum int64    // sum of leaf.info.Messages in subtree
}

func newTopicTree(topics []rp.TopicInfo) *topicTree {
	t := &topicTree{
		root:     &topicNode{children: map[string]*topicNode{}},
		expanded: map[string]bool{},
	}
	for _, info := range topics {
		t.insert(info)
	}
	t.finalize(t.root)
	// Expand the first level by default — collapsed-all is rarely what the
	// user wants when they enter the screen.
	for _, k := range t.root.childKeys {
		c := t.root.children[k]
		if !c.isLeaf {
			t.expanded[c.fullPath] = true
		}
	}
	return t
}

func (t *topicTree) insert(info rp.TopicInfo) {
	segments := strings.Split(info.Name, ".")
	cur := t.root
	path := ""
	for i, seg := range segments {
		if path == "" {
			path = seg
		} else {
			path = path + "." + seg
		}
		isLast := i == len(segments)-1
		child, ok := cur.children[seg]
		if !ok {
			child = &topicNode{
				name:     seg,
				fullPath: path,
				depth:    i + 1,
				children: map[string]*topicNode{},
			}
			cur.children[seg] = child
		}
		if isLast {
			child.isLeaf = true
			child.info = info
		}
		cur = child
	}
}

// finalize sorts children for stable rendering and computes aggregated stats.
// Idempotent — safe to call on a partial tree, used after every insert batch.
func (t *topicTree) finalize(n *topicNode) {
	n.childKeys = n.childKeys[:0]
	for k := range n.children {
		n.childKeys = append(n.childKeys, k)
	}
	sort.Strings(n.childKeys)

	n.leafCount = 0
	n.messagesSum = 0
	if n.isLeaf {
		n.leafCount = 1
		n.messagesSum = n.info.Messages
	}
	for _, k := range n.childKeys {
		c := n.children[k]
		t.finalize(c)
		n.leafCount += c.leafCount
		n.messagesSum += c.messagesSum
	}
}

// Toggle flips the expand state of a branch node. No-op for leaves.
func (t *topicTree) Toggle(fullPath string) {
	t.expanded[fullPath] = !t.expanded[fullPath]
}

// ExpandAll opens every branch.
func (t *topicTree) ExpandAll() {
	var walk func(n *topicNode)
	walk = func(n *topicNode) {
		if !n.isLeaf && n != t.root {
			t.expanded[n.fullPath] = true
		}
		for _, k := range n.childKeys {
			walk(n.children[k])
		}
	}
	walk(t.root)
}

// CollapseAll closes every branch.
func (t *topicTree) CollapseAll() {
	for k := range t.expanded {
		delete(t.expanded, k)
	}
}

// IsExpanded reports whether a branch is currently expanded.
func (t *topicTree) IsExpanded(fullPath string) bool {
	return t.expanded[fullPath]
}

// VisibleRows walks the tree depth-first and yields the rows that should be
// rendered given the current expand state and an optional substring filter.
// An empty filter shows the current expand state as-is; otherwise the tree is
// projected to the paths that match the filter, with every ancestor branch
// implicitly expanded so matches are reachable.
func (t *topicTree) VisibleRows(filter string) []*topicNode {
	filter = strings.ToLower(strings.TrimSpace(filter))
	if filter == "" {
		var out []*topicNode
		t.walkVisible(t.root, &out)
		return out
	}
	matchedAncestors := map[string]bool{}
	var matched []*topicNode
	t.walkMatch(t.root, filter, matchedAncestors, &matched)
	if len(matched) == 0 {
		return nil
	}
	prev := t.expanded
	t.expanded = matchedAncestors
	var out []*topicNode
	t.walkVisible(t.root, &out)
	t.expanded = prev
	// Keep only matched leaves + their ancestor branches in output.
	keep := map[string]bool{}
	for _, m := range matched {
		keep[m.fullPath] = true
		segments := strings.Split(m.fullPath, ".")
		for i := 1; i < len(segments); i++ {
			keep[strings.Join(segments[:i], ".")] = true
		}
	}
	filtered := out[:0]
	for _, n := range out {
		if keep[n.fullPath] {
			filtered = append(filtered, n)
		}
	}
	return filtered
}

func (t *topicTree) walkVisible(n *topicNode, out *[]*topicNode) {
	if n != t.root {
		*out = append(*out, n)
	}
	if n.isLeaf {
		return
	}
	if n == t.root || t.expanded[n.fullPath] {
		for _, k := range n.childKeys {
			t.walkVisible(n.children[k], out)
		}
	}
}

func (t *topicTree) walkMatch(
	n *topicNode, filter string, ancestors map[string]bool, matched *[]*topicNode,
) {
	if n.isLeaf {
		if strings.Contains(strings.ToLower(n.info.Name), filter) {
			*matched = append(*matched, n)
		}
		return
	}
	for _, k := range n.childKeys {
		c := n.children[k]
		before := len(*matched)
		t.walkMatch(c, filter, ancestors, matched)
		if len(*matched) > before && !c.isLeaf {
			ancestors[c.fullPath] = true
		}
	}
}
