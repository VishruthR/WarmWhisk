#!/bin/bash
set -euo pipefail

COUCHDB_URL="http://whisk_admin:some_passw0rd@127.0.0.1:5984"
ACTIVATIONS_DB="whisk_local_activations"

# Fetch all activations with duration and action name in one query
ALL_DOCS=$(curl -s -X POST -H "Content-Type: application/json" \
  -d '{
    "selector": { "duration": { "$gt": 0 } },
    "fields": ["name", "duration", "activationId", "response.statusCode"],
    "limit": 999999
  }' \
  "$COUCHDB_URL/$ACTIVATIONS_DB/_find")

NUM_DOCS=$(echo "$ALL_DOCS" | jq '.docs | length')

if [ "$NUM_DOCS" -eq 0 ]; then
  echo "No activations found."
  exit 0
fi

echo "Total activations: $NUM_DOCS"
echo ""

# Use jq to compute stats for all activations and per-action in one pass
echo "$ALL_DOCS" | jq -r '
def percentile(arr; p):
  (arr | sort) as $sorted |
  ((($sorted | length) - 1) * p) as $idx |
  ($idx | floor) as $lo |
  ($idx | ceil) as $hi |
  if $lo == $hi then $sorted[$lo]
  else ($sorted[$lo] + $sorted[$hi]) / 2
  end;

def stats(durations):
  (durations | add / length) as $avg |
  percentile(durations; 0.5) as $p50 |
  percentile(durations; 0.90) as $p90 |
  percentile(durations; 0.99) as $p99 |
  (durations | min) as $min |
  (durations | max) as $max |
  { count: (durations | length), avg: ($avg * 100 | round / 100), p50: $p50, p90: $p90, p99: $p99, min: $min, max: $max };

.docs as $docs |

# Overall stats
($docs | map(.duration)) as $all_durations |
stats($all_durations) as $overall |

# Per-action stats
($docs | group_by(.name) | map({
  name: .[0].name,
  stats: stats(map(.duration))
}) | sort_by(.name)) as $by_action |

# Print overall
"=== Overall ===",
"  Count:   \($overall.count)",
"  Average: \($overall.avg)ms",
"  P50:     \($overall.p50)ms",
"  P90:     \($overall.p90)ms",
"  P99:     \($overall.p99)ms",
"  Min:     \($overall.min)ms",
"  Max:     \($overall.max)ms",
"",
"=== By Action ===",
($by_action[] |
  "",
  "  \(.name) (n=\(.stats.count))",
  "    Average: \(.stats.avg)ms",
  "    P50:     \(.stats.p50)ms",
  "    P90:     \(.stats.p90)ms",
  "    P99:     \(.stats.p99)ms",
  "    Min:     \(.stats.min)ms",
  "    Max:     \(.stats.max)ms"
)
'
