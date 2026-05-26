package db

import "testing"

func TestDeriveDirectURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "neon pooler",
			in:   "postgresql://user:pw@ep-example-abc123-pooler.c-2.us-east-1.aws.neon.tech/db?sslmode=require",
			want: "postgresql://user:pw@ep-example-abc123.c-2.us-east-1.aws.neon.tech/db?sslmode=require",
		},
		{
			name: "neon pooler with port",
			in:   "postgresql://user:pw@ep-xyz-pooler.example.neon.tech:5432/db",
			want: "postgresql://user:pw@ep-xyz.example.neon.tech:5432/db",
		},
		{
			name: "no pooler suffix passes through",
			in:   "postgresql://user:pw@ep-xyz.example.neon.tech/db?sslmode=require",
			want: "postgresql://user:pw@ep-xyz.example.neon.tech/db?sslmode=require",
		},
		{
			name: "non-neon host passes through",
			in:   "postgresql://user:pw@db.internal:5432/mydb",
			want: "postgresql://user:pw@db.internal:5432/mydb",
		},
		{
			name: "pooler appearing mid-hostname is not a Neon suffix",
			in:   "postgresql://user:pw@some-pooler-host.example.com/db",
			want: "postgresql://user:pw@some-pooler-host.example.com/db",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveDirectURL(tc.in)
			if got != tc.want {
				t.Errorf("deriveDirectURL(%q)\n  got:  %q\n  want: %q", tc.in, got, tc.want)
			}
		})
	}
}
