package postgres

type GeoLevel struct {
	BaseModel
	Name string   `gorm:"type:varchar(64);unique;not null" json:"name"`
	Rank *float64 `gorm:"type:float" json:"rank"`
}
