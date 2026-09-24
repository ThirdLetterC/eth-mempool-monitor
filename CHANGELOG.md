# Changelog

All notable changes to this project are documented in this file. Release dates
and links are based on the published [GitHub releases][releases]; change details
are summarized from the commits contained in each tagged release.

## [v1.2.6] - 2026-09-24

### Added

- Added the `http_transmitter` binary for forwarding monitored-transaction
  events from RabbitMQ to a predefined HTTP or HTTPS webhook as JSON.
- Added optional environment-backed bearer authentication, bounded retry and
  exponential backoff, request timeouts, and at-least-once delivery through
  manual RabbitMQ acknowledgements.
- Added pinned libcurl integration, Docker and release packaging, configuration
  documentation, and loopback webhook regression tests.

### Security

- Reject malformed or oversized RabbitMQ payloads without requeue, preserve TLS
  peer and hostname verification, and prevent bearer-token forwarding through
  HTTP redirects.

## [v1.2.5] - 2026-09-24

### Added

- Added exact human-readable Gwei gas prices and maximum pending transaction
  fees to monitored-transaction log lines while retaining the original
  hexadecimal quantities.
- Added checked fee arithmetic and full-range Ethereum quantity formatting
  tests, including overflow rejection.

## [v1.2.4] - 2026-09-24

### Added

- Added exact human-readable ETH amounts to monitored-transaction log lines
  while retaining the original hexadecimal wei quantity.
- Added bounded, resumable RPC batching for multi-million-address TOML imports.

### Changed

- Changed the default public Ethereum WebSocket endpoint to PublicNode.

## [v1.2.3] - 2026-09-24

### Added

- Added a bounded-memory Binance proof-of-reserves importer that streams large
  archives into monitor-compatible TOML address files.
- Added `podman-compose` 1.6.0 to the locked development toolchain.

### Fixed

- Improved Podman compatibility for Compose image names and sysctl settings.
- Increased the WebSocket subscriber receive buffer from 64 KiB to 256 KiB so
  large pending-transaction notifications are not prematurely discarded.

## [v1.2.1] - 2026-09-24

### Fixed

- Made optimized builds use the baseline CPU model by default so published
  x86-64 binaries do not inherit unsupported instructions from CI runners.
- Added release checks that reject YMM/ZMM instructions and smoke-test every
  executable before upload.

## [v1.2.0] - 2026-09-24

### Added

- Added compiler-distinct domain types for ports, time units, RabbitMQ
  channels, prefetch counts, and socket backlogs.
- Added typed status codes for WebSocket, subscriber, RabbitMQ, configuration,
  console, monitor runtime, and RPC-control operations.
- Added first-party C type-safety tests through the `zig build test` step.

### Changed

- Consolidated the subscriber entry points into a structured options API.
- Made RabbitMQ publishing asynchronous with a bounded replay queue, publisher
  confirms, and retry backoff.
- Updated the build for Zig 0.16 and centralized POSIX feature flags.
- Converted monitored-address configuration from text files to TOML.
- Updated the Python tooling and CI baseline to Python 3.14.
- Enabled stricter integer-conversion and enum-conversion diagnostics for
  first-party C sources.

### Performance

- Removed hot-path buffer allocations from WebSocket and transaction-processing
  paths.

### Maintenance

- Removed unused first-party includes using clangd include-cleaner diagnostics.

## [v1.1.1] - 2026-09-23

### Fixed

- Bounded runtime resource usage, including connection state, request buffers,
  and message-processing limits.
- Documented module trust boundaries, ownership, and cleanup responsibilities.

## [v1.1.0] - 2026-09-23

### Added

- Added tagged GitHub releases with Linux x86-64 binaries and SHA-256
  checksums.
- Added optional mimalloc integration across the executables and bundled
  libraries.
- Added CI checks for C compilation, Python quality, Markdown, and YAML.
- Added production Docker hardening and expanded operational documentation.

### Changed

- Migrated the build to Zig 0.16 and hardened the C build configuration.
- Reorganized the monitor, WebSocket, RabbitMQ, RPC-control, and Python client
  implementations into smaller modules with internal headers.
- Replaced traditional project-header include guards with `#pragma once`.
- Improved wolfSSL compatibility and Python RPC client validation.

## [v1.0.0] - 2026-02-07

### Added

- Initial release of the Ethereum pending-transaction monitor.
- Added WebSocket subscription support with optional TLS through wolfSSL.
- Added Redis-backed address filtering and RabbitMQ event publishing.
- Added the authenticated JSON-RPC control service and Python client.
- Added the RabbitMQ transaction console, TOML configuration, Docker images,
  Compose deployments, and Zig build support.

[releases]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases
[v1.0.0]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.0.0
[v1.1.0]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.1.0
[v1.1.1]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.1.1
[v1.2.0]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.0
[v1.2.1]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.1
[v1.2.3]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.3
[v1.2.4]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.4
[v1.2.5]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.5
[v1.2.6]: https://github.com/ThirdLetterC/eth-mempool-monitor/releases/tag/v1.2.6
