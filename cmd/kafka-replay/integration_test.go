// Integration tests for the CLI. They build the binary, run it with various
// arguments, and assert on exit codes and output. No Kafka cluster required
// for the cases covered here (error paths and cat with a local file).
package main_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lolocompany/kafka-replay/pkg/transcoder"
)

var binaryPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kafka-replay-integration-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	binaryPath = filepath.Join(dir, "kafka-replay")
	if err := exec.Command("go", "build", "-o", binaryPath, ".").Run(); err != nil {
		panic("build failed: " + err.Error())
	}
	os.Exit(m.Run())
}

func runCLI(args ...string) (stdout, stderr []byte, exitCode int) {
	cmd := exec.Command(binaryPath, args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	stdout = outBuf.Bytes()
	stderr = errBuf.Bytes()
	if err == nil {
		return stdout, stderr, 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return stdout, stderr, ee.ExitCode()
	}
	return stdout, stderr, -1
}

func TestCLI_ListBrokers_NoBrokers_Exit1(t *testing.T) {
	_, stderr, code := runCLI("list", "brokers")
	if code != 1 {
		t.Errorf("expected exit 1, got %d", code)
	}
	if !strings.Contains(string(stderr), "no brokers") {
		t.Errorf("stderr should mention no brokers; got %q", string(stderr))
	}
}

func TestCLI_ListBrokers_ConnectivityFailure_Exit3(t *testing.T) {
	_, stderr, code := runCLI("--brokers", "localhost:19999", "list", "brokers")
	if code != 3 {
		t.Errorf("expected exit 3 (connectivity), got %d", code)
	}
	if !strings.Contains(string(stderr), "failed to connect") && !strings.Contains(string(stderr), "dial") {
		t.Errorf("stderr should mention connection failure; got %q", string(stderr))
	}
}

func TestCLI_ListPartitions_OutputJSON(t *testing.T) {
	// Without a real broker we get exit 3; we only check that --format=json is accepted
	stdout, stderr, code := runCLI("--brokers", "localhost:19999", "--format=json", "list", "partitions")
	if code != 3 {
		t.Errorf("expected exit 3 (no broker), got %d", code)
	}
	if len(stdout) != 0 {
		t.Errorf("stdout should be empty on error; got %q", string(stdout))
	}
	_ = stderr
}

func TestCLI_Cat_MissingInput_Exit1(t *testing.T) {
	_, stderr, code := runCLI("cat")
	if code != 1 {
		t.Errorf("expected exit 1, got %d", code)
	}
	if !strings.Contains(string(stderr), "input") && !strings.Contains(string(stderr), "required") {
		t.Errorf("stderr should mention input/required; got %q", string(stderr))
	}
}

func TestCLI_Cat_InvalidFormat_Exit1(t *testing.T) {
	f, err := os.CreateTemp("", "cat-input-*")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(f.Name())
	_, _, code := runCLI("--format=invalid", "cat", "--input", f.Name())
	if code != 1 {
		t.Errorf("expected exit 1 for invalid --format, got %d", code)
	}
}

func TestCLI_Cat_OutputJSON(t *testing.T) {
	path := createMessageFile(t, []byte("key1"), []byte("hello"))
	defer os.Remove(path)
	stdout, stderr, code := runCLI("--format=json", "cat", "--input", path)
	if code != 0 {
		t.Fatalf("cat json: exit %d, stderr %q", code, string(stderr))
	}
	lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 JSON line, got %d", len(lines))
	}
	var obj struct {
		Timestamp string `json:"timestamp"`
		Key       string `json:"key"`
		Data      string `json:"data"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &obj); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if obj.Key != "key1" || obj.Data != "hello" {
		t.Errorf("expected key=key1 data=hello, got key=%q data=%q", obj.Key, obj.Data)
	}
}

func TestCLI_Cat_OutputRaw(t *testing.T) {
	payload := []byte("raw-payload")
	path := createMessageFile(t, []byte(""), payload)
	defer os.Remove(path)
	stdout, _, code := runCLI("--format=raw", "cat", "--input", path)
	if code != 0 {
		t.Fatalf("cat raw: exit %d", code)
	}
	if !bytes.Equal(stdout, payload) {
		t.Errorf("raw output: got %q", string(stdout))
	}
}

func TestCLI_Cat_Count(t *testing.T) {
	path := createMessageFile(t, []byte("a"), []byte("b"))
	defer os.Remove(path)
	stdout, stderr, code := runCLI("cat", "--input", path, "--count")
	if code != 0 {
		t.Fatalf("cat --count: exit %d, stderr %q", code, string(stderr))
	}
	trimmed := strings.TrimSpace(string(stdout))
	if trimmed != "1" {
		t.Errorf("expected stdout '1', got %q", trimmed)
	}
}

func TestCLI_ExitCode_Usage(t *testing.T) {
	_, _, code := runCLI("list", "brokers") // no brokers
	if code != 1 {
		t.Errorf("no brokers should be exit 1 (usage), got %d", code)
	}
}

func TestCLI_ListTopics(t *testing.T) {
	// No brokers: exit 1
	_, stderr, code := runCLI("list", "topics")
	if code != 1 {
		t.Errorf("expected exit 1 without brokers, got %d", code)
	}
	if !strings.Contains(string(stderr), "no brokers") {
		t.Errorf("stderr should mention no brokers; got %q", string(stderr))
	}
}

func TestCLI_ListConsumerGroups(t *testing.T) {
	// No brokers: exit 1
	_, stderr, code := runCLI("list", "consumer-groups")
	if code != 1 {
		t.Errorf("expected exit 1 without brokers, got %d", code)
	}
	if !strings.Contains(string(stderr), "no brokers") {
		t.Errorf("stderr should mention no brokers; got %q", string(stderr))
	}
}

func TestCLI_Record(t *testing.T) {
	// Missing required --topic: exit 1
	_, stderr, code := runCLI("record", "--output", "/tmp/out.log")
	if code != 1 {
		t.Errorf("expected exit 1 without --topic, got %d", code)
	}
	if !strings.Contains(string(stderr), "topic") && !strings.Contains(string(stderr), "required") {
		t.Errorf("stderr should mention topic/required; got %q", string(stderr))
	}
}

func TestCLI_Replay(t *testing.T) {
	// Missing required --topic: exit 1
	_, stderr, code := runCLI("replay", "--input", "/dev/null")
	if code != 1 {
		t.Errorf("expected exit 1 without --topic, got %d", code)
	}
	if !strings.Contains(string(stderr), "topic") && !strings.Contains(string(stderr), "required") {
		t.Errorf("stderr should mention topic/required; got %q", string(stderr))
	}
}

func TestCLI_Config(t *testing.T) {
	// debug config shows resolved config (config file, profile, brokers) and their sources
	stdout, stderr, code := runCLI("debug", "config")
	if code != 0 {
		t.Errorf("debug config: expected exit 0, got %d, stderr %q", code, string(stderr))
	}
	combined := string(stdout) + string(stderr)
	for _, want := range []string{"config file", "profile", "brokers"} {
		if !strings.Contains(combined, want) {
			t.Errorf("debug config output should contain %q; got:\n%s", want, combined)
		}
	}
}

func TestCLI_Config_LoadConfigFile(t *testing.T) {
	// Write a YAML config file and run debug config --config <path>; brokers from file should appear.
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	const yamlContent = `
default_profile: prod
profiles:
  prod:
    brokers:
      - kafka1.example.com:9092
      - kafka2.example.com:9092
  dev:
    brokers:
      - localhost:9092
`
	if err := os.WriteFile(configPath, []byte(yamlContent), 0600); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runCLI("debug", "config", "--config", configPath)
	if code != 0 {
		t.Fatalf("debug config with file: exit %d, stderr %q", code, string(stderr))
	}
	combined := string(stdout) + string(stderr)
	if !strings.Contains(combined, configPath) {
		t.Errorf("output should show config file path; got:\n%s", combined)
	}
	if !strings.Contains(combined, "kafka1.example.com:9092") || !strings.Contains(combined, "kafka2.example.com:9092") {
		t.Errorf("output should show brokers from config profile; got:\n%s", combined)
	}
	if !strings.Contains(combined, "prod") {
		t.Errorf("output should show default profile prod; got:\n%s", combined)
	}
}

func TestCLI_InspectTopic(t *testing.T) {
	// Missing topic name: exit 1 or error
	_, stderr, code := runCLI("--brokers", "localhost:19999", "inspect", "topic")
	if code != 1 && code != 2 {
		t.Errorf("inspect topic without name: expected exit 1 or 2, got %d", code)
	}
	if !strings.Contains(string(stderr), "topic") && !strings.Contains(string(stderr), "required") && !strings.Contains(string(stderr), "required") {
		t.Logf("stderr: %q", string(stderr))
	}
}

func TestCLI_InspectConsumerGroup(t *testing.T) {
	// Missing group ID: exit 1
	_, stderr, code := runCLI("--brokers", "localhost:19999", "inspect", "consumer-group")
	if code != 1 {
		t.Errorf("inspect consumer-group without ID: expected exit 1, got %d", code)
	}
	if !strings.Contains(string(stderr), "consumer group") && !strings.Contains(string(stderr), "required") {
		t.Logf("stderr: %q", string(stderr))
	}
}

func TestCLI_Debug(t *testing.T) {
	// debug runs and shows help or subcommands
	_, stderr, code := runCLI("debug")
	if code != 0 {
		t.Errorf("debug: expected exit 0, got %d, stderr %q", code, string(stderr))
	}
}

func TestCLI_Version(t *testing.T) {
	stdout, stderr, code := runCLI("version")
	if code != 0 {
		t.Errorf("version: expected exit 0, got %d, stderr %q", code, string(stderr))
	}
	combined := string(stdout) + string(stderr)
	if !strings.Contains(combined, "version") && !strings.Contains(combined, "kafka-replay") {
		t.Errorf("version output should mention version or kafka-replay; got %q", combined)
	}
}

// createMessageFile writes a single message in v2 format and returns the path.
func createMessageFile(t *testing.T, key, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "kafka-replay-cat-*")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	enc, err := transcoder.NewEncodeWriter(f)
	if err != nil {
		f.Close()
		os.Remove(path)
		t.Fatal(err)
	}
	_, err = enc.Write(time.Unix(0, 0), data, key)
	if err != nil {
		f.Close()
		os.Remove(path)
		t.Fatal(err)
	}
	// enc.Close() closes the underlying file
	if err := enc.Close(); err != nil {
		os.Remove(path)
		t.Fatal(err)
	}
	return path
}
