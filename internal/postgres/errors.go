package postgres

import "errors"

var (
	ErrPrimaryNameExists     = errors.New("location already has a primary name")
	ErrInvalidHierarchy      = errors.New("parent rank must be lower than child rank")
	ErrDuplicateRelation     = errors.New("child already has a parent of this level")
	ErrLocationNotFound      = errors.New("location not found")
	ErrGeoLevelNotExist      = errors.New("geo level does not exist")
	ErrNameRequired          = errors.New("name is required")
	ErrNameAlreadyExists     = errors.New("name already exists for this location")
	ErrCannotDeletePrimary   = errors.New("cannot delete primary name")
	ErrPrimaryNameNotFound   = errors.New("primary name not found for location")
	ErrGeoLevelNameRequired  = errors.New("geo level name is required")
	ErrGeoLevelNameNotUpper  = errors.New("geo level name must be uppercase")
	ErrGeoLevelAlreadyExists = errors.New("geo level with this name already exists")
	ErrGeoLevelNotFound      = errors.New("geo level not found")
	ErrGeoLevelInUse         = errors.New("geo level is in use by locations and cannot be deleted")
	ErrRelationNotFound      = errors.New("relation not found")
)
