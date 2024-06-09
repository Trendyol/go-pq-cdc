package integration

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	Config    config.Config
	Container testcontainers.Container
)

func TestMain(m *testing.M) {
	var err error
	Config, err = config.ReadConfigYaml("./test_connector.yaml")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	Container, err = SetupTestContainer(ctx, Config)
	if err != nil {
		log.Fatal("setup test container", err)
	}
	defer func() {
		err = Container.Terminate(ctx)
		if err != nil {
			log.Fatal("terminate test container", err)
		}
	}()

	Config.Host, err = Container.ContainerIP(ctx)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := newPostgresConn()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = conn.Close(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	err = SetupTestDB(ctx, conn, Config)
	if err != nil {
		log.Fatal(err)
	}

	m.Run()
}

func SetupTestContainer(ctx context.Context, cfg config.Config) (testcontainers.Container, error) {
	req, err := containerRequest(cfg)
	if err != nil {
		return nil, err
	}

	return testcontainers.GenericContainer(ctx, req)
}

func containerRequest(cfg config.Config) (testcontainers.GenericContainerRequest, error) {
	req := testcontainers.ContainerRequest{
		Image: "docker.io/postgres:16-alpine",
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       cfg.Database,
		},
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          []string{"postgres", "-c", "fsync=off", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"},
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	err := testcontainers.WithWaitStrategy(
		wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5 * time.Second)).Customize(&genericContainerReq)
	if err != nil {
		return testcontainers.GenericContainerRequest{}, err
	}

	return genericContainerReq, nil
}

func newPostgresConn() (pq.Connection, error) {
	return pq.NewConnection(context.TODO(), config.Config{Host: Config.Host, Username: "postgres", Password: "postgres", Database: Config.Database})
}

func SetupTestDB(ctx context.Context, conn pq.Connection, cfg config.Config) error {
	if err := createCDCUser(ctx, conn, cfg); err != nil {
		return err
	}

	if err := createBooksTable(ctx, conn); err != nil {
		return err
	}

	if err := dropPublication(ctx, conn, cfg); err != nil {
		return err
	}

	if err := createPublication(ctx, conn, cfg); err != nil {
		return err
	}

	if err := createReplicationSlot(ctx, conn, cfg); err != nil {
		return err
	}

	return nil
}

func RestoreDB(ctx context.Context) error {
	conn, err := newPostgresConn()
	if err != nil {
		return err
	}
	defer func() {
		err = conn.Close(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}()

	if err = createBooksTable(ctx, conn); err != nil {
		return err
	}

	return nil
}

func createCDCUser(ctx context.Context, conn pq.Connection, cfg config.Config) error {
	commands := []string{
		fmt.Sprintf("CREATE USER %s WITH REPLICATION PASSWORD '%s';", cfg.Username, cfg.Password),
		fmt.Sprintf("GRANT CONNECT ON DATABASE cdc_db TO %s;", cfg.Username),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s;", cfg.Username),
	}

	for _, command := range commands {
		err := pgExec(ctx, conn, command)
		if err != nil {
			return errors.Wrap(err, "create cdc user")
		}
	}

	return nil
}

func createBooksTable(ctx context.Context, conn pq.Connection) error {
	query := `
		DROP TABLE IF EXISTS books;
		CREATE TABLE books (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		);
	`
	err := pgExec(ctx, conn, query)
	if err != nil {
		return errors.Wrap(err, "create books table")
	}

	return nil
}

func dropPublication(ctx context.Context, conn pq.Connection, cfg config.Config) error {
	err := pgExec(ctx, conn, "DROP PUBLICATION IF EXISTS "+cfg.Publication.Name)
	if err != nil {
		return errors.Wrap(err, "drop publication")
	}

	return nil
}

func createPublication(ctx context.Context, conn pq.Connection, cfg config.Config) error {
	err := pgExec(ctx, conn, "CREATE PUBLICATION "+cfg.Publication.Name+" FOR TABLE books;")
	if err != nil {
		return errors.Wrap(err, "create publication")
	}

	return nil
}

func createReplicationSlot(ctx context.Context, conn pq.Connection, cfg config.Config) error {
	err := pgExec(ctx, conn, "CREATE_REPLICATION_SLOT "+cfg.Slot.Name+" LOGICAL pgoutput;")
	if err != nil {
		return errors.Wrap(err, "create replication slot")
	}

	return nil
}

func pgExec(ctx context.Context, conn pq.Connection, command string) error {
	resultReader := conn.Exec(ctx, command)
	_, err := resultReader.ReadAll()
	if err != nil {
		return err
	}

	if err = resultReader.Close(); err != nil {
		return err
	}

	return nil
}

func fetchDeleteOpMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_delete_total")
	mi, _ := strconv.Atoi(m)
	return mi, err
}

func fetchInsertOpMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_insert_total")
	mi, _ := strconv.Atoi(m)
	return mi, err
}

func fetchUpdateOpMetric() (int, error) {
	m, err := fetchMetrics("go_pq_cdc_update_total")
	mi, _ := strconv.Atoi(m)
	return mi, err
}

func fetchMetrics(metricName string) (string, error) {
	resp, err := http.Get("http://localhost:8083/metrics")
	if err != nil {
		return "", errors.Wrap(err, "error fetching metrics")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.Newf("error: received status code %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "error reading response body")
	}

	bodyStr := string(body)
	lines := strings.Split(bodyStr, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, metricName) {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				return parts[1], nil
			}
		}
	}
	return "", errors.Newf("metric %s not found in response", metricName)
}
