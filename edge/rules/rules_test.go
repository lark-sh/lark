package rules

import (
	"sort"
	"testing"
)

func TestGetVolatilePaths(t *testing.T) {
	tests := []struct {
		name     string
		rules    string
		expected []string
	}{
		{
			name:     "empty rules",
			rules:    "{}",
			expected: nil,
		},
		{
			name:     "empty string",
			rules:    "",
			expected: nil,
		},
		{
			name: "single volatile path",
			rules: `{
				"players": {
					"$playerId": {
						"position": {
							".volatile": true
						}
					}
				}
			}`,
			expected: []string{"players/*/position"},
		},
		{
			name: "multiple volatile paths",
			rules: `{
				"players": {
					"$playerId": {
						"position": {
							".volatile": true
						},
						"rotation": {
							".volatile": true
						}
					}
				}
			}`,
			expected: []string{"players/*/position", "players/*/rotation"},
		},
		{
			name: "with rules wrapper",
			rules: `{
				"rules": {
					"cursors": {
						"$cursorId": {
							".volatile": true
						}
					}
				}
			}`,
			expected: []string{"cursors/*"},
		},
		{
			name: "no volatile paths",
			rules: `{
				"messages": {
					".read": "true",
					".write": "auth != null"
				}
			}`,
			expected: nil,
		},
		{
			name: "mixed exact and wildcard children",
			rules: `{
				"game": {
					"state": {
						".read": "true"
					},
					"$playerId": {
						"cursor": {
							".volatile": true
						}
					}
				}
			}`,
			expected: []string{"game/*/cursor"},
		},
		{
			name: "nested volatile path",
			rules: `{
				"rooms": {
					"$roomId": {
						"players": {
							"$playerId": {
								"position": {
									".volatile": true
								}
							}
						}
					}
				}
			}`,
			expected: []string{"rooms/*/players/*/position"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetVolatilePaths(tt.rules)

			// Sort both for comparison (map iteration order is random)
			sort.Strings(result)
			sort.Strings(tt.expected)

			if len(result) != len(tt.expected) {
				t.Errorf("GetVolatilePaths() = %v, want %v", result, tt.expected)
				return
			}

			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("GetVolatilePaths() = %v, want %v", result, tt.expected)
					return
				}
			}
		})
	}
}

func TestGetVolatilePathsInvalidJSON(t *testing.T) {
	result := GetVolatilePaths("not valid json")
	if result != nil {
		t.Errorf("Expected nil for invalid JSON, got %v", result)
	}
}

func TestGetVolatilePathsWithComments(t *testing.T) {
	tests := []struct {
		name     string
		rules    string
		expected []string
	}{
		{
			name: "line comment at start",
			rules: `// This is a comment
{
	"players": {
		"$playerId": {
			"position": {
				".volatile": true
			}
		}
	}
}`,
			expected: []string{"players/*/position"},
		},
		{
			name: "line comment in middle",
			rules: `{
	// Player data
	"players": {
		"$playerId": {
			"position": {
				".volatile": true // cursor position
			}
		}
	}
}`,
			expected: []string{"players/*/position"},
		},
		{
			name: "block comment",
			rules: `{
	/* Player tracking
	   with volatile cursor */
	"players": {
		"$playerId": {
			"position": {
				".volatile": true
			}
		}
	}
}`,
			expected: []string{"players/*/position"},
		},
		{
			name: "comment-like sequence in string",
			rules: `{
	"players": {
		"$playerId": {
			"bio": {
				".read": "auth != null",
				".write": "auth.uid == $playerId // owner only"
			},
			"cursor": {
				".volatile": true
			}
		}
	}
}`,
			expected: []string{"players/*/cursor"},
		},
		{
			name: "Example style rules header",
			rules: `//auth.currentcampaign = player or gm in this campaign
{
  "rules": {
    "cursors": {
      "$cursorId": {
        ".volatile": true
      }
    }
  }
}`,
			expected: []string{"cursors/*"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetVolatilePaths(tt.rules)

			sort.Strings(result)
			sort.Strings(tt.expected)

			if len(result) != len(tt.expected) {
				t.Errorf("GetVolatilePaths() = %v, want %v", result, tt.expected)
				return
			}

			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("GetVolatilePaths() = %v, want %v", result, tt.expected)
					return
				}
			}
		})
	}
}

func TestStripJSONComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no comments",
			input:    `{"key": "value"}`,
			expected: `{"key": "value"}`,
		},
		{
			name:     "line comment",
			input:    "// comment\n{\"key\": \"value\"}",
			expected: "\n{\"key\": \"value\"}",
		},
		{
			name:     "block comment",
			input:    "/* comment */{\"key\": \"value\"}",
			expected: "{\"key\": \"value\"}",
		},
		{
			name:     "comment in string preserved",
			input:    `{"key": "// not a comment"}`,
			expected: `{"key": "// not a comment"}`,
		},
		{
			name:     "escaped quote in string",
			input:    `{"key": "value \"with\" quotes // still string"}`,
			expected: `{"key": "value \"with\" quotes // still string"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripJSONComments(tt.input)
			if result != tt.expected {
				t.Errorf("stripJSONComments() = %q, want %q", result, tt.expected)
			}
		})
	}
}
