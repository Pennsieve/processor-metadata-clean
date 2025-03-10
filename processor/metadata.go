package processor

import (
	"fmt"
	changesetmodels "github.com/pennsieve/processor-post-metadata/client/models"
	"log/slog"
)

func (p *MetadataCleanProcessor) GetCleanSet() (*changesetmodels.Dataset, error) {
	var modelDeletes []changesetmodels.ModelDelete
	for name, id := range p.MetadataReader.Schema.ModelIDsByName() {
		modelLogger := logger.With(slog.String("name", name),
			slog.String("id", id))
		modelLogger.Info("adding model to clean set")
		var toDelete []changesetmodels.PennsieveInstanceID
		records, err := p.MetadataReader.GetRecordsForModel(name)
		if err != nil {
			return nil, fmt.Errorf("error reading records for model %s (%s): %w",
				name, id, err)
		}
		for _, r := range records {
			toDelete = append(toDelete, changesetmodels.PennsieveInstanceID(r.ID))
		}
		modelLogger.Info("adding records to model clean set",
			slog.Int("toDelete", len(toDelete)))
		modelDeletes = append(modelDeletes, changesetmodels.ModelDelete{
			ID:      changesetmodels.PennsieveSchemaID(id),
			Records: toDelete,
		})
	}
	return &changesetmodels.Dataset{
		Models: changesetmodels.ModelChanges{
			Deletes: modelDeletes,
		},
	}, nil
}
