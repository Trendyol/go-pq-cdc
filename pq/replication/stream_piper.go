package replication

import "context"

type streamRunner func(ctx context.Context)
