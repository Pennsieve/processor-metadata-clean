package processor

import (
	"fmt"
	"os"
)

const IntegrationIDKey = "INTEGRATION_ID"
const InputDirectoryKey = "INPUT_DIR"
const OutputDirectoryKey = "OUTPUT_DIR"

func FromEnv() (*MetadataCleanProcessor, error) {
	integrationID, err := LookupRequiredEnvVar(IntegrationIDKey)
	if err != nil {
		return nil, err
	}
	inputDirectory, err := LookupRequiredEnvVar(InputDirectoryKey)
	if err != nil {
		return nil, err
	}
	outputDirectory, err := LookupRequiredEnvVar(OutputDirectoryKey)
	if err != nil {
		return nil, err
	}
	return NewMetadataCleanProcessor(integrationID,
		inputDirectory,
		outputDirectory,
	)
}

func LookupRequiredEnvVar(key string) (string, error) {
	value := os.Getenv(key)
	if len(value) == 0 {
		return "", fmt.Errorf("no %s set", key)
	}
	return value, nil
}
