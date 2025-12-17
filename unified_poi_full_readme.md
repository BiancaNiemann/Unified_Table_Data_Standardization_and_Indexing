# 🗺️ Unified POI Table

> A comprehensive data pipeline for consolidating Berlin's Point of Interest (POI) data into a single unified table with spatial indexing and nearest-neighbor analysis.

## Overview

This pipeline standardizes and consolidates multiple POI datasets from across Berlin into a single, spatially-indexed table optimized for analysis and querying. Built with Python, SQLAlchemy, and PostGIS.

## Features

- ✅ **Automated validation** of source tables before processing
- 🔄 **Standardized schema** across all POI types
- 📍 **Spatial indexing** with PostGIS for fast geospatial queries
- 🎯 **Nearest neighbor analysis** for proximity calculations
- 🔧 **Flexible attributes** stored as JSONB for schema extension
- ⚡ **Single transaction** execution for atomicity and performance

## Table of Contents

- [Schema Definition](#schema-definition)
- [Data Cleaning](#data-cleaning)
- [Pipeline Architecture](#pipeline-architecture)
- [Implementation Steps](#implementation-steps)
- [Getting Started](#getting-started)

## Schema Definition

All source tables are standardized to a common schema before consolidation.

### Source Table Schema

```sql
CREATE TABLE source_poi (
    id VARCHAR(20) PRIMARY KEY,              -- Numeric only, no letters
    district_id VARCHAR(20) NOT NULL,        -- Mapped from district name
    name VARCHAR(200) NOT NULL,              -- Defaults to 'Unknown' if NULL
    latitude DECIMAL(9,6),                      
    longitude DECIMAL(9,6), 
    geometry VARCHAR,                        -- POINT() format
    neighborhood VARCHAR(100),               -- Joined from neighborhoods table
    district VARCHAR(100),                   -- From lor_ortsteile.geojson
    neighborhood_id VARCHAR(20),             -- From lor_ortsteile.geojson
    
    CONSTRAINT district_id_fk 
        FOREIGN KEY (district_id)
        REFERENCES berlin_source_data.districts(district_id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);
```

### Unified POI Schema

```sql
CREATE TABLE unified_pois (
    poi_id VARCHAR(50) PRIMARY KEY,          -- Format: [layer_prefix]-[original_id]
    name VARCHAR(200),  
    layer VARCHAR(100),                      -- Source table name
    district_id VARCHAR(20),
    district VARCHAR(100),
    neighborhood_id VARCHAR(20),
    neighborhood VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    geometry GEOMETRY,                       -- PostGIS geometry type
    attributes JSONB,                        -- Non-standard columns
    nearest_pois JSONB                       -- Nearest POI per layer
);
```

## Data Cleaning

The pipeline includes comprehensive data cleaning to ensure all source tables meet schema requirements.

### Standardization Operations

**Table Renaming**
```sql
-- Rename to layer names for clarity
ALTER TABLE berlin_source_data.hospitals_refactored 
RENAME TO hospitals;
```

**Column Renaming**
```sql
-- Standardize column naming conventions
ALTER TABLE berlin_source_data.food_markets 
RENAME COLUMN market_id TO id;
```

**Data Type Corrections**
```sql
-- Fix inconsistent data types
ALTER TABLE berlin_source_data.short_term_listings 
ALTER COLUMN id TYPE VARCHAR(20) USING id::VARCHAR(20);

ALTER TABLE berlin_source_data.short_term_listings 
ALTER COLUMN name TYPE VARCHAR(200) USING name::VARCHAR(200);
```

**Missing Column Addition**
```sql
-- Add missing latitude/longitude columns
ALTER TABLE berlin_source_data.bike_lanes 
ADD COLUMN latitude VARCHAR NULL;

ALTER TABLE berlin_source_data.bike_lanes 
ADD COLUMN longitude VARCHAR NULL;
```

### Data Enrichment

The `table_corrections` Jupyter notebook handles:

- **Missing neighborhood_id**: Joins neighborhood names to lookup tables
- **Missing geometry**: Generates POINT() format from lat/lon coordinates
- **Missing location data**: Uses Nominatim API for geocoding
- **Inconsistent district IDs**: Corrects outdated district numbering
- **Incomplete records**: Recreates tables with missing required columns

## Pipeline Architecture

**Technology Stack**
- Language: Python 3.x
- ORM: SQLAlchemy
- Database: PostgreSQL with PostGIS extension
- Pattern: Single transaction for atomicity

**File Structure**
```
├── create_unified_poi_table.py    # Main pipeline script
├── table_corrections.ipynb        # Data cleaning notebook
└── README.md                      # This file
```

## Implementation Steps

### Step 1: Validation

Creates an `excluded_tables_log` to identify invalid tables before processing.

**Validation Checks:**
- Required columns present
- Correct data types
- No NULL values in non-nullable columns
- PRIMARY KEY constraint on `id`
- FOREIGN KEY constraint on `district_id`

**Process Flow:**
1. Query all tables in `berlin_source_data` schema
2. Compare against expected schema definition
3. Check for missing columns, type mismatches, NULL issues
4. Verify constraint existence
5. Log invalid tables for exclusion

### Step 2: Table Creation

Creates two pipeline tables:

**`unified_pois`** - Main consolidated POI table

**`processed_tables_log`** - Tracks successfully processed source tables

### Step 3: Table Selection

Queries `berlin_source_data` schema to identify valid tables:
- Excludes `districts` and `neighborhoods` reference tables
- Excludes tables logged in `excluded_tables_log`

### Step 4: Data Consolidation & Enrichment

For each valid source table:

**Data Transformation**
- Generate unique `poi_id` (format: `[layer]-[id]`)
- Convert geometry to SRID 4326
- Serialize non-standard columns to JSONB `attributes`

**Union Operation**
- Combine all transformed tables into single dataset

**Nearest Neighbor Analysis** *(long_term_listings only)*
- Calculate nearest POI across all other layers
- Store as JSONB with POI ID, name, distance, and address
- Populate `nearest_pois` column

**Data Insertion**
- Insert consolidated POIs into `unified_pois`
- Log processed tables in `processed_tables_log`

### Step 5: Spatial Indexing

Creates a GiST spatial index for optimized geospatial queries:

```sql
CREATE INDEX idx_poi_geom 
ON unified_pois 
USING GIST (geometry);
```

## Example Query

```sql
-- Find all POIs within 500m of a location
SELECT poi_id, name, layer, 
       ST_Distance(geometry, ST_SetSRID(ST_MakePoint(13.404954, 52.520008), 4326)::geography) as distance_m
FROM unified_pois
WHERE ST_DWithin(
    geometry::geography, 
    ST_SetSRID(ST_MakePoint(13.404954, 52.520008), 4326)::geography,
    500
)
ORDER BY distance_m;
```

## Performance Optimizations

- ⚡ Single transaction minimizes connection overhead
- 🏃 GiST spatial index enables fast distance-based queries
- 🔧 JSONB attributes provide flexible schema extension
- 📐 PostGIS operators optimize nearest neighbor calculations

