# Location Service

The Location Service provides a structured way to represent and manage geographical entities and their relationships for enterprises. It is designed to handle complex location hierarchies and mappings efficiently.

## Components

### 1. Geo Level

- **Definition:** Represents the level or type of a place (e.g., COUNTRY, STATE, DISTRICT, CIRCLE, etc.).
- **Fields:**
  - `name`: Unique identifier for the geo level, always in uppercase (e.g., "COUNTRY").
  - `rank`: Nullable float value that determines the hierarchy of the level (lower rank = higher in hierarchy).

### 2. Location

- **Definition:** Represents an identifiable unit of a location, such as a country, state, or any other geo level.
- **Fields:**
  - `geo_id`: Unique identifier for the location. (uuid)
  - `geo_level`: The geo level this location belongs to.
  - `name`: Name of the location.
  - `aliases`: Alternative names by which the location is known.

### 3. Inclusion Map

- **Definition:** Represents the relationship between two locations, defining hierarchical inclusion.
- **Fields:**
  - `parent_location`: The parent location in the relationship.
  - `child_location`: The child location in the relationship.
- **Rules:**
  - The parent location's level determines the type of relationship.
  - A child location can have a particular relation with only one parent (e.g., a state can belong to only one country).(Approach in case of a sql db, is to write a custom trigger that, on insert or update of rows in the geo_map table, performs a query joining the location table to verify that the combination of child and the parent's level is unique among all geo_map rows)

---

This service enables enterprises to model, query, and manage complex geographical hierarchies and relationships with flexibility and precision.
