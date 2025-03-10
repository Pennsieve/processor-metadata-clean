package processor

import (
	"encoding/json"
	"fmt"
	"github.com/pennsieve/processor-metadata-clean/logging"
	changeset "github.com/pennsieve/processor-post-metadata/client"
	changesetmodels "github.com/pennsieve/processor-post-metadata/client/models"
	metadataclient "github.com/pennsieve/processor-pre-metadata/client"
	"log/slog"
	"os"
	"path/filepath"
)

var logger = logging.PackageLogger("processor")

type MetadataCleanProcessor struct {
	IntegrationID   string
	InputDirectory  string
	OutputDirectory string
	MetadataReader  *metadataclient.Reader
}

func NewMetadataCleanProcessor(integrationID string, inputDirectory string, outputDirectory string) (*MetadataCleanProcessor, error) {
	reader, err := metadataclient.NewReader(inputDirectory)
	if err != nil {
		return nil, fmt.Errorf("error creating metadata reader for %s: %w", inputDirectory, err)
	}
	return &MetadataCleanProcessor{
		IntegrationID:   integrationID,
		InputDirectory:  inputDirectory,
		OutputDirectory: outputDirectory,
		MetadataReader:  reader,
	}, nil
}

func (p *MetadataCleanProcessor) Run() error {
	logger.Info("starting metadata clean processing")
	logger.Info("creating clean instructions")
	cleanSet, err := p.GetCleanSet()
	if err != nil {
		return err
	}
	if err := p.writeChangeset(cleanSet); err != nil {
		return err
	}
	logger.Info("finished metadata clean processing")
	return nil
}

func (p *MetadataCleanProcessor) ChangesetFilePath() string {
	return filepath.Join(p.OutputDirectory, changeset.Filename)
}

func (p *MetadataCleanProcessor) writeChangeset(changes *changesetmodels.Dataset) error {
	filePath := p.ChangesetFilePath()
	file, err := os.Create(filePath)
	defer func() {
		if err := file.Close(); err != nil {
			logger.Warn("error closing changeset file", slog.String("path", filePath), slog.Any("error", err))
		}
	}()
	if err != nil {
		return fmt.Errorf("error creating changeset file %s: %w", filePath, err)
	}
	if err := json.NewEncoder(file).Encode(changes); err != nil {
		return fmt.Errorf("error writing changeset file: %s: %w", filePath, err)
	}
	logger.Info("wrote changeset file", slog.String("path", filePath))
	return nil
}
