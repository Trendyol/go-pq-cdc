# Preserved reconnect tests before the online POC

The user explicitly approved archiving these seven untracked tests before feature implementation. They depended on reconnect APIs absent from tracked baseline `6110b91596ea9597b588954f94a89a1b5f4cfcd6`, or asserted reconnect behavior that failed on that baseline.

The contents are preserved byte-for-byte under their former paths with a `.txt` suffix. They are not compiled by Go. No tracked tests were removed or disabled.

| Former path | SHA-256 |
|---|---|
| `config/slot_config_test.go` | `4538be98e2ce8882394b6abb89197996a628e9842ce374d3c8aa93240f7c44fc` |
| `pq/connection_test.go` | `567aeee90c4cb8686a3d051c9713704d92967a0ae424d1425a1c372254fa6e4e` |
| `pq/slot/config_test.go` | `c982933d4ec6ebe29e5ed6c1363f7296406dd97b94bf51f4cc5034139a51117b` |
| `pq/slot/recovery_test.go` | `bed190da342bb76fd9e1a7e6cb1a70f1a1d01867b2d4766bd9a7c629c3ef757e` |
| `pq/slot/wire_test.go` | `ac55be431b7f9dcfbace528b8861385f958795be60cb97d7ac9d84a573cd136f` |
| `integration_test/replication_disconnect_test.go` | `c334d1d16753078baabf8cbe4a64b057ff1600f226c445371d8a0fb3ae15f54e` |
| `integration_test/slot_info_reconnect_test.go` | `b8cedf81189269d68013790a23d68b6e6313c38cd1d8df03fd433da17b04929f` |

After archiving, root `go test -race -count=1 ./...`, `go vet ./...`, and four selected PostgreSQL 16 integration tests passed. The full integration suite remains an implementation validation gate.
