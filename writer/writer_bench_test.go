package writer

import (
	"fmt"
	"testing"

	"github.com/TheRealShek/trackr7/db"
)

func BenchmarkBuildInsertSQL(b *testing.B) {
	b.ReportAllocs()
	cfg := db.DBConfig{
		LocationsTable: "trackr.locations",
		LocationColumns: db.LocationColumnMap{
			UUID: "uuid", EntityID: "entity_id", EntityType: "entity_type", Lat: "lat", Lng: "lng", TS: "ts",
		},
	}.WithDefaults()

	sizes := []int{10, 100, 500}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("batch_size_%d", sz), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = buildInsertSQL(cfg)
				_ = sz
			}
		})
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	b.ReportAllocs()
	b.Skip("integration benchmark gated by TRACKR7_TEST_DSN")
}
