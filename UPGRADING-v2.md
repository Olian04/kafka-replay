# Upgrading to Kafka Replay v2

This guide helps you migrate from v1 to v2. **There is no compatibility mode:** scripts and invocations must be updated to the new flags, global options, and JSON field names.

## Command and flag mapping

| v1 | v2 |
|----|-----|
| `kafka-replay list brokers --broker HOST` | `kafka-replay --brokers HOST list brokers` |
| `kafka-replay list partitions --broker HOST` | `kafka-replay --brokers HOST list partitions` |
| `kafka-replay list consumer-groups --broker HOST` | `kafka-replay --brokers HOST list consumer-groups` |
| `kafka-replay record --broker HOST --topic T ...` | `kafka-replay --brokers HOST record --topic T ...` |
| `kafka-replay replay --broker HOST --topic T ...` | `kafka-replay --brokers HOST replay --topic T ...` |
| `kafka-replay cat --input F --raw` | `kafka-replay cat --input F --format raw` |
| (no equivalent) | `kafka-replay list topics` |
| (no equivalent) | `kafka-replay debug config` (shows resolved config file, profile, brokers and their sources) |
| (no equivalent) | `kafka-replay inspect topic TOPIC` |
| (no equivalent) | `kafka-replay inspect consumer-group GROUP_ID` |

## Examples

### Brokers and list commands

**v1:**
```bash
kafka-replay list partitions --broker host:9092
kafka-replay list consumer-groups --broker host:9092 --offsets
```

**v2:**
```bash
kafka-replay --brokers host:9092 list partitions
kafka-replay --brokers host:9092 list consumer-groups --offsets
```

You can still use the `KAFKA_BROKERS` environment variable (comma-separated). Global `--brokers` takes precedence, then config profile, then env. Global flags (e.g. `--brokers`, `--format`, `--quiet`) can appear before or after the command name.

### Record and replay

**v1:**
```bash
kafka-replay record --broker host:9092 --topic my-topic --output out.log
kafka-replay replay --broker host:9092 --topic my-topic --input out.log
```

**v2:**
```bash
kafka-replay --brokers host:9092 record --topic my-topic --output out.log
kafka-replay --brokers host:9092 replay --topic my-topic --input out.log
```

### Cat output format

**v1:**
```bash
kafka-replay cat --input messages.log --raw
```

**v2:**
```bash
kafka-replay cat --input messages.log --format raw
```

Output formats in v2 are `table` (default for list/inspect), `json` (one JSON object per line), and `raw` (cat only). Use `--format json` for script-friendly list output; cat defaults to `json` when no format is set.

## JSON output field name changes

Scripts that parse JSON from `list` commands must use the new field names.

### List brokers

| v1 | v2 |
|----|-----|
| `address` | `address` (unchanged) |
| `reachable` | `reachable` (unchanged) |
| (not present) | `id` (broker ID) |
| (not present) | `rack` (optional) |

### List partitions

| v1 | v2 |
|----|-----|
| `topic` | `topic` (unchanged) |
| `partitions` | `partition` (single partition ID) |
| `leader` | `leader` (unchanged) |
| `replicatedOnBrokers` | `followers` |
| `earliest` | `earliestOffset` |
| `latest` | `latestOffset` |
| `replicas` | `replicas` (unchanged) |
| `inSyncReplicas` | `inSyncReplicas` (unchanged) |

### List consumer-groups

| v1 | v2 |
|----|-----|
| `group` | `groupId` |
| `state` | `state` (unchanged) |
| `protocolType` | `protocolType` (unchanged) |
| `members` | `members` (unchanged) |
| `offsets` | `offsets` (unchanged) |

Member and offset nested fields (`memberId`, `clientId`, `topic`, `partition`, `offset`, etc.) are unchanged.

## Example: updating a jq script

**v1 (consumer group names):**
```bash
kafka-replay list consumer-groups --broker localhost:9092 | jq -r '.group'
```

**v2:**
```bash
kafka-replay --brokers localhost:9092 --format json list consumer-groups | jq -r '.groupId'
```

**v1 (partition list):**
```bash
kafka-replay list partitions --broker localhost:9092 | jq -r '.topic + " " + (.partitions | tostring)'
```

**v2:**
```bash
kafka-replay --brokers localhost:9092 --format json list partitions | jq -r '.topic + " " + (.partition | tostring)'
```

### Quiet mode

Use global `--quiet` to suppress all status and progress output from `record` and `replay` (e.g. "Recording messages from...", progress spinner, "Recorded N messages"). Useful for scripting.

```bash
kafka-replay --brokers localhost:9092 --quiet record --topic my-topic --output out.log
```

### Resolved config

To see which config file, profile, and brokers are in effect (and where each value comes from), run:

```bash
kafka-replay debug config
```

## Exit codes

v2 uses consistent exit codes for scripting:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Usage or config error (invalid flags, missing args, bad config file) |
| 2 | Not found (e.g. topic or consumer group not found for `inspect`) |
| 3 | Network or Kafka connectivity/auth error |

## No compatibility mode

v2 does not support a `--compat-v1` flag or similar. All invocations and scripts must be updated as above.
