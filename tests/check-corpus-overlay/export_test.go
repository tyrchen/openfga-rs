package check

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/tests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type corpusEvent struct {
	Kind      string          `json:"kind"`
	StoreID   string          `json:"storeId,omitempty"`
	ModelID   string          `json:"modelId,omitempty"`
	Request   json.RawMessage `json:"request,omitempty"`
	Allowed   *bool           `json:"allowed,omitempty"`
	ErrorCode int             `json:"errorCode,omitempty"`
}

type corpusRecorder struct {
	tests.ClientInterface
	mu     sync.Mutex
	events []corpusEvent
}

func (r *corpusRecorder) CreateStore(
	ctx context.Context,
	in *openfgav1.CreateStoreRequest,
	opts ...grpc.CallOption,
) (*openfgav1.CreateStoreResponse, error) {
	response, err := r.ClientInterface.CreateStore(ctx, in, opts...)
	if err == nil {
		r.append(corpusEvent{Kind: "createStore", StoreID: response.GetId()})
	}
	return response, err
}

func (r *corpusRecorder) WriteAuthorizationModel(
	ctx context.Context,
	in *openfgav1.WriteAuthorizationModelRequest,
	opts ...grpc.CallOption,
) (*openfgav1.WriteAuthorizationModelResponse, error) {
	response, err := r.ClientInterface.WriteAuthorizationModel(ctx, in, opts...)
	if err != nil {
		return response, err
	}
	request, marshalErr := marshalProto(in)
	if marshalErr != nil {
		return nil, marshalErr
	}
	r.append(corpusEvent{
		Kind:    "writeModel",
		StoreID: in.GetStoreId(),
		ModelID: response.GetAuthorizationModelId(),
		Request: request,
	})
	return response, nil
}

func (r *corpusRecorder) Write(
	ctx context.Context,
	in *openfgav1.WriteRequest,
	opts ...grpc.CallOption,
) (*openfgav1.WriteResponse, error) {
	response, err := r.ClientInterface.Write(ctx, in, opts...)
	if err != nil {
		return response, err
	}
	request, marshalErr := marshalProto(in)
	if marshalErr != nil {
		return nil, marshalErr
	}
	r.append(corpusEvent{Kind: "writeTuples", StoreID: in.GetStoreId(), Request: request})
	return response, nil
}

func (r *corpusRecorder) Check(
	ctx context.Context,
	in *openfgav1.CheckRequest,
	opts ...grpc.CallOption,
) (*openfgav1.CheckResponse, error) {
	response, err := r.ClientInterface.Check(ctx, in, opts...)
	request, marshalErr := marshalProto(in)
	if marshalErr != nil {
		return nil, marshalErr
	}
	event := corpusEvent{
		Kind:      "check",
		StoreID:   in.GetStoreId(),
		ModelID:   in.GetAuthorizationModelId(),
		Request:   request,
		ErrorCode: int(status.Code(err)),
	}
	if err == nil {
		allowed := response.GetAllowed()
		event.Allowed = &allowed
	}
	r.append(event)
	return response, err
}

func (r *corpusRecorder) append(event corpusEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *corpusRecorder) write(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := json.Marshal(r.events)
	if err != nil {
		return fmt.Errorf("marshal corpus events: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write corpus events: %w", err)
	}
	return nil
}

func marshalProto(message proto.Message) (json.RawMessage, error) {
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf request: %w", err)
	}
	return data, nil
}

func TestExportCheckCorpus(t *testing.T) {
	output := os.Getenv("OPENFGA_CHECK_CORPUS_OUTPUT")
	if output == "" {
		t.Fatal("OPENFGA_CHECK_CORPUS_OUTPUT is required")
	}
	baseline := tests.BuildClientInterface(t, "memory", []string{config.ExperimentalCheckOptimizations})
	recorder := &corpusRecorder{ClientInterface: baseline}
	t.Cleanup(func() {
		if err := recorder.write(output); err != nil {
			t.Errorf("write recorded Check corpus: %v", err)
		}
	})

	t.Run("assets", func(t *testing.T) {
		runTests(t, testParams{schemaVersion: "1.1", client: recorder})
	})
	t.Run("matrix", func(t *testing.T) {
		runTestMatrix(t, testParams{schemaVersion: "1.1", client: recorder})
	})
	t.Run("matrix_contextual", func(t *testing.T) {
		runTestMatrix(t, testParams{schemaVersion: "1.1", contextual: true, client: recorder})
	})
	t.Run("condition_any", func(t *testing.T) {
		runConditionAnyTests(t, recorder)
	})
}
