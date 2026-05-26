// Package rules provides simplified rules parsing for the proxy.
// It only extracts volatile paths - no expression evaluation.
package rules

import (
	"encoding/json"
	"strings"
)

// ruleNode represents a node in the rules tree (internal).
type ruleNode struct {
	Volatile bool
	Children map[string]*ruleNode
	Wildcard *ruleNode
}

// GetVolatilePaths parses rules JSON and returns all volatile paths.
// Wildcard segments (like $playerId) are converted to "*" for pattern matching.
// Returns paths like: ["players/*/position", "players/*/rotation"]
// Returns nil if rules are empty or invalid.
func GetVolatilePaths(rulesJSON string) []string {
	if rulesJSON == "" || rulesJSON == "{}" {
		return nil
	}

	// Strip comments before parsing (Firebase rules allow JS-style comments)
	cleaned := stripJSONComments(rulesJSON)

	var raw map[string]any
	if err := json.Unmarshal([]byte(cleaned), &raw); err != nil {
		return nil
	}

	// Check for "rules" wrapper
	if rulesMap, ok := raw["rules"].(map[string]any); ok {
		raw = rulesMap
	}

	root := parseNode(raw)
	if root == nil {
		return nil
	}

	var paths []string
	collectVolatilePaths(root, "", &paths)
	return paths
}

// parseNode recursively parses a rules node from JSON.
func parseNode(data map[string]any) *ruleNode {
	node := &ruleNode{
		Children: make(map[string]*ruleNode),
	}

	for key, value := range data {
		switch key {
		case ".volatile":
			if b, ok := value.(bool); ok {
				node.Volatile = b
			}

		case ".read", ".write", ".validate", ".indexOn":
			// Ignore rule expressions - we only care about structure and volatile

		default:
			// Child node
			if strings.HasPrefix(key, ".") {
				// Unknown directive - skip
				continue
			}

			childData, ok := value.(map[string]any)
			if !ok {
				continue
			}

			childNode := parseNode(childData)
			if childNode == nil {
				continue
			}

			if strings.HasPrefix(key, "$") {
				// Wildcard child
				node.Wildcard = childNode
			} else {
				node.Children[key] = childNode
			}
		}
	}

	return node
}

// collectVolatilePaths recursively walks the rule tree collecting volatile paths.
func collectVolatilePaths(node *ruleNode, currentPath string, paths *[]string) {
	if node.Volatile {
		// Trim leading slash for cleaner patterns
		path := strings.TrimPrefix(currentPath, "/")
		if path == "" {
			path = "*" // Root is volatile (unusual but handle it)
		}
		*paths = append(*paths, path)
	}

	// Visit exact children
	for key, child := range node.Children {
		childPath := currentPath + "/" + key
		collectVolatilePaths(child, childPath, paths)
	}

	// Visit wildcard child (convert $varName to *)
	if node.Wildcard != nil {
		childPath := currentPath + "/*"
		collectVolatilePaths(node.Wildcard, childPath, paths)
	}
}

// stripJSONComments removes JS-style comments from JSON.
// Handles both // line comments and /* block comments */.
// Correctly handles strings containing comment-like sequences.
func stripJSONComments(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	i := 0
	for i < len(s) {
		// Check for string start
		if s[i] == '"' {
			// Copy string including quotes, handling escapes
			result.WriteByte(s[i])
			i++
			for i < len(s) {
				if s[i] == '\\' && i+1 < len(s) {
					// Escaped character - copy both
					result.WriteByte(s[i])
					result.WriteByte(s[i+1])
					i += 2
				} else if s[i] == '"' {
					// End of string
					result.WriteByte(s[i])
					i++
					break
				} else {
					result.WriteByte(s[i])
					i++
				}
			}
			continue
		}

		// Check for line comment //
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '/' {
			// Skip until end of line
			i += 2
			for i < len(s) && s[i] != '\n' {
				i++
			}
			// Keep the newline for line number preservation
			if i < len(s) {
				result.WriteByte('\n')
				i++
			}
			continue
		}

		// Check for block comment /* */
		if i+1 < len(s) && s[i] == '/' && s[i+1] == '*' {
			// Skip until */
			i += 2
			for i+1 < len(s) {
				if s[i] == '*' && s[i+1] == '/' {
					i += 2
					break
				}
				// Preserve newlines for line number preservation
				if s[i] == '\n' {
					result.WriteByte('\n')
				}
				i++
			}
			continue
		}

		// Regular character
		result.WriteByte(s[i])
		i++
	}

	return result.String()
}
