package processor

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/pennsieve/processor-metadata-clean/logging"
	changesetmodels "github.com/pennsieve/processor-post-metadata/client/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"slices"
	"testing"
)

func TestCurationExportSyncProcessor_Run(t *testing.T) {
	currentLogLevel := logging.Level.Level()
	logging.Level.Set(slog.LevelDebug)
	t.Cleanup(func() {
		logging.Level.Set(currentLogLevel)
	})
	integrationID := uuid.NewString()
	inputDirectory := "testdata/input"
	outputDirectory := t.TempDir()

	processor, err := NewMetadataCleanProcessor(integrationID, inputDirectory, outputDirectory)
	require.NoError(t, err)

	require.NoError(t, processor.Run())

	assert.FileExists(t, processor.ChangesetFilePath())

	// Check changes contents
	changesetFile, err := os.Open(processor.ChangesetFilePath())
	require.NoError(t, err)
	defer changesetFile.Close()
	var changeset changesetmodels.Dataset
	require.NoError(t, json.NewDecoder(changesetFile).Decode(&changeset))

	assert.Empty(t, changeset.Models.Creates)
	assert.Empty(t, changeset.Models.Updates)
	assert.Len(t, changeset.Models.Deletes, 3)

	// Contributors
	contributorDeletes := findModelDeleteByID(t, changeset.Models, "d77470bb-f39d-49ee-aa17-783e128cdfa0")
	assert.Len(t, contributorDeletes.Records, 5)

	// Subjects
	subjectDeletes := findModelDeleteByID(t, changeset.Models, "44fe1f90-f7b5-407a-8689-c512d7f41b7d")
	assert.Len(t, subjectDeletes.Records, 3)

	// Samples
	sampleDeletes := findModelDeleteByID(t, changeset.Models, "29756423-00de-42f8-8706-acdcb1823685")
	assert.Len(t, sampleDeletes.Records, 3)

	// Links
	assert.Empty(t, changeset.LinkedProperties)

	// Proxies
	assert.Nil(t, changeset.Proxies)
}

func findModelDeleteByID(t *testing.T, modelChanges changesetmodels.ModelChanges, modelID string) changesetmodels.ModelDelete {
	index := slices.IndexFunc(modelChanges.Deletes, func(changes changesetmodels.ModelDelete) bool {
		return changes.ID.String() == modelID
	})
	require.GreaterOrEqual(t, index, 0)
	return modelChanges.Deletes[index]
}
