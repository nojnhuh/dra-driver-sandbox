package driver

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"

	checkpointapi "github.com/nojnhuh/dra-driver-sandbox/internal/api/checkpoint/v1"
)

func checkpointSerializer() (runtime.Decoder, runtime.Encoder, error) {
	builder := runtime.NewSchemeBuilder(
		checkpointapi.AddToScheme,
	)
	checkpointScheme := runtime.NewScheme()
	if err := builder.AddToScheme(checkpointScheme); err != nil {
		return nil, nil, err
	}

	checkpointJSON := json.NewSerializerWithOptions(
		json.DefaultMetaFactory,
		checkpointScheme,
		checkpointScheme,
		json.SerializerOptions{Pretty: true},
	)
	checkpointCodecFactory := serializer.NewCodecFactory(checkpointScheme)
	checkpointEncoder := checkpointCodecFactory.EncoderForVersion(checkpointJSON, checkpointapi.SchemeGroupVersion)
	// When different API versions may be stored on disk, this should refer to
	// the version to and from which all other versions can convert.
	checkpointDecoder := checkpointCodecFactory.UniversalDecoder(checkpointapi.SchemeGroupVersion)

	return checkpointDecoder, checkpointEncoder, nil
}

// readCheckpoint returns the Checkpoint at the given path in the format
// expected by the given decoder. If the path doesn't exist, returns an empty
// Checkpoint and no error.
func readCheckpoint(path string, decoder runtime.Decoder) (*checkpointapi.Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	checkpoint := new(checkpointapi.Checkpoint)
	if data != nil {
		_, _, err = decoder.Decode(data, new(checkpointapi.SchemeGroupVersion.WithKind("Checkpoint")), checkpoint)
		if err != nil {
			return nil, fmt.Errorf("unmarshal JSON from %s: %w", path, err)
		}
	}
	return checkpoint, nil
}

// writeCheckpoint writes checkpoint to the file at path in the format
// prescribed by encoder. The file is overwritten if it already exists and is
// created if it does not already exist.
func writeCheckpoint(path string, encoder runtime.Encoder, checkpoint *checkpointapi.Checkpoint) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "dra-sandbox-checkpoint-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	defer func() {
		if err1 := tmp.Close(); err1 != nil && err == nil {
			err = fmt.Errorf("close temp file: %w", err1)
		}
	}()
	if err := encoder.Encode(checkpoint, tmp); err != nil {
		return fmt.Errorf("encode to temp file %s: %w", tmp.Name(), err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename %s to %s: %w", tmp.Name(), path, err)
	}
	return nil
}
