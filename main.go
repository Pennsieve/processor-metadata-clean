package main

import (
	"github.com/pennsieve/processor-metadata-clean/logging"
	"github.com/pennsieve/processor-metadata-clean/processor"
	"log/slog"
	"os"
)

var logger = logging.PackageLogger("main")

func main() {
	m, err := processor.FromEnv()
	if err != nil {
		logger.Error("error creating processor", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("created MetadataCleanProcessor",
		slog.String("integrationID", m.IntegrationID),
		slog.String("inputDirectory", m.InputDirectory),
		slog.String("outputDirectory", m.OutputDirectory),
	)

	if err := m.Run(); err != nil {
		logger.Error("error running processor", slog.Any("error", err))
		os.Exit(1)
	}
}
