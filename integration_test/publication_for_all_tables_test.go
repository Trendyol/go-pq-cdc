package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublicationForAllTablesEmptyIncludedTables tests the fix for issue #50:
// When a publication is created with "FOR ALL TABLES" and there are no tables yet
// (empty included_tables), the Info() method should correctly handle this case
// and return an empty tables array instead of failing.
func TestPublicationForAllTablesEmptyIncludedTables(t *testing.T) {
	ctx := context.Background()

	publicationName := "test_pub_for_all_tables_empty"
	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	// Cleanup: Drop publication if it exists
	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	})

	// Create a publication with "FOR ALL TABLES" using raw SQL
	// This simulates a publication created externally or before any tables exist
	createPubQuery := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate')",
		publicationName,
	)
	err = pgExec(ctx, postgresConn, createPubQuery)
	require.NoError(t, err, "Failed to create publication FOR ALL TABLES")

	// Create publication config for querying
	pubConfig := publication.Config{
		Name: publicationName,
	}

	// Create publication instance
	pub := publication.New(pubConfig, postgresConn)

	// Test: Query the publication info - this should work correctly
	// The fix ensures that Info() doesn't fail when querying "FOR ALL TABLES" publications,
	// even when there are no tables or when tables exist. The key is that the query handles
	// the COALESCE correctly and doesn't fail on empty results.
	info, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed - this is the main fix: handling FOR ALL TABLES correctly")

	// Verify the publication info
	assert.Equal(t, publicationName, info.Name, "Publication name should match")
	// Tables may be nil, empty, or contain existing tables (like 'books' from other tests)
	// The important thing is that Info() succeeds and returns valid data
	assert.NotNil(t, info.Operations, "Operations should not be nil")
	assert.Contains(t, info.Operations, publication.OperationInsert, "Should include INSERT operation")
	assert.Contains(t, info.Operations, publication.OperationUpdate, "Should include UPDATE operation")
	assert.Contains(t, info.Operations, publication.OperationDelete, "Should include DELETE operation")
	assert.Contains(t, info.Operations, publication.OperationTruncate, "Should include TRUNCATE operation")
	
	// The fix ensures that even if tables is empty/nil, the query doesn't fail
	// We just verify that the structure is valid (nil or a slice)
	if info.Tables != nil {
		// If tables exist, verify they are properly formatted (schema.table was split correctly)
		for _, table := range info.Tables {
			assert.NotEmpty(t, table.Name, "Table name should not be empty")
			assert.NotEmpty(t, table.Schema, "Table schema should not be empty")
		}
	}
}

// TestPublicationForAllTablesWithTables tests that when a publication is created
// with "FOR ALL TABLES" and tables exist, the Info() method correctly returns
// all tables in the database.
func TestPublicationForAllTablesWithTables(t *testing.T) {
	ctx := context.Background()

	publicationName := "test_pub_for_all_tables_with_tables"
	tableName1 := "test_table_1"
	tableName2 := "test_table_2"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	// Cleanup
	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName1))
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName2))
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	})

	// Create test tables first
	createTable1Query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		)
	`, tableName1)
	err = pgExec(ctx, postgresConn, createTable1Query)
	require.NoError(t, err, "Failed to create table 1")

	createTable2Query := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			value TEXT NOT NULL
		)
	`, tableName2)
	err = pgExec(ctx, postgresConn, createTable2Query)
	require.NoError(t, err, "Failed to create table 2")

	// Create a publication with "FOR ALL TABLES"
	createPubQuery := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES WITH (publish = 'insert, update, delete')",
		publicationName,
	)
	err = pgExec(ctx, postgresConn, createPubQuery)
	require.NoError(t, err, "Failed to create publication FOR ALL TABLES")

	// Create publication config for querying
	pubConfig := publication.Config{
		Name: publicationName,
	}

	// Create publication instance
	pub := publication.New(pubConfig, postgresConn)

	// Test: Query the publication info
	info, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed")

	// Verify the publication info
	assert.Equal(t, publicationName, info.Name, "Publication name should match")
	require.NotNil(t, info.Tables, "Tables should not be nil when tables exist")
	
	// Verify that both tables are included in the publication
	// Note: FOR ALL TABLES includes all tables in the database, so there might be more
	// We just verify that our test tables are included
	tableNames := make(map[string]bool)
	for _, table := range info.Tables {
		tableNames[table.Name] = true
	}

	assert.True(t, tableNames[tableName1], "Table 1 should be included in publication")
	assert.True(t, tableNames[tableName2], "Table 2 should be included in publication")
	assert.Contains(t, info.Operations, publication.OperationInsert, "Should include INSERT operation")
	assert.Contains(t, info.Operations, publication.OperationUpdate, "Should include UPDATE operation")
	assert.Contains(t, info.Operations, publication.OperationDelete, "Should include DELETE operation")
}

// TestPublicationForAllTablesEmptyThenAddTables tests the scenario where:
// 1. Publication is created with "FOR ALL TABLES" when no tables exist (empty included_tables)
// 2. Tables are added later
// 3. Info() should correctly return the tables after they are added
func TestPublicationForAllTablesEmptyThenAddTables(t *testing.T) {
	ctx := context.Background()

	publicationName := "test_pub_for_all_tables_progressive"
	tableName := "test_table_progressive"

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	// Cleanup
	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName))
	})

	// Step 1: Create publication with "FOR ALL TABLES" when no tables exist
	createPubQuery := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR ALL TABLES WITH (publish = 'insert, update')",
		publicationName,
	)
	err = pgExec(ctx, postgresConn, createPubQuery)
	require.NoError(t, err, "Failed to create publication FOR ALL TABLES")

	pubConfig := publication.Config{
		Name: publicationName,
	}
	pub := publication.New(pubConfig, postgresConn)

	// Step 2: Query info initially - may include existing tables (like 'books' from test setup)
	info1, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed")
	
	// Get initial table count (may include existing tables)
	info1Len := 0
	initialTableNames := make(map[string]bool)
	if info1.Tables != nil {
		info1Len = len(info1.Tables)
		for _, table := range info1.Tables {
			initialTableNames[table.Name] = true
		}
	}
	
	// Verify our test table is not in the initial list
	assert.False(t, initialTableNames[tableName], "Test table should not exist initially")

	// Step 3: Create a table (it will automatically be included in FOR ALL TABLES publication)
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			data TEXT NOT NULL
		)
	`, tableName)
	err = pgExec(ctx, postgresConn, createTableQuery)
	require.NoError(t, err, "Failed to create table")

	// Step 4: Query info again - should now include the new table
	info2, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed after table creation")
	require.NotNil(t, info2.Tables, "Tables should not be nil after table creation")

	// Verify the table is now included
	tableNames := make(map[string]bool)
	for _, table := range info2.Tables {
		tableNames[table.Name] = true
	}
	assert.True(t, tableNames[tableName], "Newly created table should be included in publication")
	
	// Verify the count increased by at least 1
	assert.GreaterOrEqual(t, len(info2.Tables), info1Len+1, "Should have at least one more table after creating one")
}

