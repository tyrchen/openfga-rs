package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/condition"
)

const (
	maxCases           = 64
	maxExpressionBytes = 4096
	maxFixtureBytes    = 65536
)

type fixture struct {
	Cases []testCase `json:"cases"`
}

type testCase struct {
	Name             string  `json:"name"`
	Expression       string  `json:"expression"`
	ExpectedBaseline outcome `json:"expectedBaseline"`
}

type outcome struct {
	Kind  string `json:"kind"`
	Value *bool  `json:"value,omitempty"`
}

type reportCase struct {
	Name    string  `json:"name"`
	Outcome outcome `json:"outcome"`
}

type report struct {
	Baseline string       `json:"baseline"`
	Cases    []reportCase `json:"cases"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "CEL baseline failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: cel-baseline <fixture.json>")
	}
	source, err := readBounded(os.Args[1])
	if err != nil {
		return err
	}
	var corpus fixture
	if err := json.Unmarshal(source, &corpus); err != nil {
		return fmt.Errorf("decode fixture: %w", err)
	}
	if len(corpus.Cases) == 0 || len(corpus.Cases) > maxCases {
		return fmt.Errorf("fixture case count must be in 1..=%d", maxCases)
	}

	result := report{Baseline: "openfga-cel-go-0.30.0", Cases: make([]reportCase, 0, len(corpus.Cases))}
	for _, test := range corpus.Cases {
		if test.Name == "" || len(test.Name) > 128 || len(test.Expression) > maxExpressionBytes {
			return fmt.Errorf("invalid bounded fixture case %q", test.Name)
		}
		actual := evaluate(test)
		if !equalOutcome(actual, test.ExpectedBaseline) {
			return fmt.Errorf("case %q: expected %+v, found %+v", test.Name, test.ExpectedBaseline, actual)
		}
		result.Cases = append(result.Cases, reportCase{Name: test.Name, Outcome: actual})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(true)
	return encoder.Encode(result)
}

func evaluate(test testCase) outcome {
	evaluable := condition.NewUncompiled(&openfgav1.Condition{
		Name:       test.Name,
		Expression: test.Expression,
	})
	result, err := evaluable.Evaluate(context.Background(), nil)
	if err != nil {
		return outcome{Kind: "error"}
	}
	value := result.ConditionMet
	return outcome{Kind: "bool", Value: &value}
}

func equalOutcome(left, right outcome) bool {
	if left.Kind != right.Kind {
		return false
	}
	if left.Value == nil || right.Value == nil {
		return left.Value == nil && right.Value == nil
	}
	return *left.Value == *right.Value
}

func readBounded(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open fixture: %w", err)
	}
	defer func() { _ = file.Close() }()
	contents, err := io.ReadAll(io.LimitReader(file, maxFixtureBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read fixture: %w", err)
	}
	if len(contents) > maxFixtureBytes {
		return nil, errors.New("fixture exceeds byte limit")
	}
	return contents, nil
}
