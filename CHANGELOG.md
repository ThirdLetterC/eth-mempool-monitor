# Changelog

All notable changes to this project are documented in this file. Release dates
and links are based on the published [GitHub releases][releases]; change details
are summarized from the commits contained in each tagged release.

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
