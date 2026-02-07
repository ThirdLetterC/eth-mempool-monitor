const std = @import("std");

pub fn build(b: *std.Build) void {
    const release_mode = b.option(
        bool,
        "release",
        "Build in release mode (equivalent to -Doptimize=ReleaseFast).",
    ) orelse false;

    const force_valgrind = b.option(
        bool,
        "valgrind",
        "Force baseline CPU features for Valgrind compatibility.",
    ) orelse false;

    const base_target_query = b.standardTargetOptionsQueryOnly(.{});
    const base_target = b.resolveTargetQuery(base_target_query);

    var valgrind_target_query = base_target_query;
    valgrind_target_query.cpu_model = .baseline;
    valgrind_target_query.cpu_features_add = .empty;
    valgrind_target_query.cpu_features_sub = .empty;
    const valgrind_target = b.resolveTargetQuery(valgrind_target_query);

    const target = if (force_valgrind) valgrind_target else base_target;
    const optimize = if (release_mode) .ReleaseFast else b.standardOptimizeOption(.{});

    const enable_sanitizers = b.option(
        bool,
        "sanitizers",
        "Enable ASan/UBSan/LSan for C sources in Debug builds.",
    ) orelse false;

    const use_mimalloc = b.option(
        bool,
        "mimalloc",
        "Enable mimalloc malloc/free override for executables.",
    ) orelse false;
    const strip_binaries = b.option(
        bool,
        "strip",
        "Strip debug symbols from produced artifacts.",
    ) orelse false;
    const use_sanitizers = enable_sanitizers and optimize == .Debug and target.result.os.tag != .windows;
    const sanitize_c = if (use_sanitizers) std.zig.SanitizeC.full else std.zig.SanitizeC.off;

    const c_flags = &[_][]const u8{
        "-std=c23",
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Werror",
    };
    const ulog_c_flags = &[_][]const u8{
        "-std=c23",
        "-Wall",
        "-Wextra",
        "-Wpedantic",
        "-Werror",
        "-DULOG_BUILD_DYNAMIC_CONFIG=1",
    };
    const hiredis_c_flags = &[_][]const u8{
        "-std=c23",
        "-D_DEFAULT_SOURCE",
        "-D_POSIX_C_SOURCE=200809L",
    };
    const rabbitmq_c_flags = &[_][]const u8{
        "-std=c23",
        "-D_DEFAULT_SOURCE",
        "-D_POSIX_C_SOURCE=200809L",
        "-DHAVE_POLL",
    };
    const jsonrpc_c_flags = &[_][]const u8{
        "-std=c23",
        "-D_DEFAULT_SOURCE",
        "-D_POSIX_C_SOURCE=200809L",
    };
    const jsonrpc_files = &[_][]const u8{
        "src/jsonrpc/arena.c",
        "src/jsonrpc/jsonrpc.c",
        "src/jsonrpc/parson.c",
        "src/jsonrpc/server.c",
    };
    const hiredis_files = &[_][]const u8{
        "src/hiredis/alloc.c",
        "src/hiredis/async.c",
        "src/hiredis/dict.c",
        "src/hiredis/hiredis.c",
        "src/hiredis/net.c",
        "src/hiredis/read.c",
        "src/hiredis/sds.c",
    };
    const rabbitmq_files = &[_][]const u8{
        "src/rabbitmq/amqp_api.c",
        "src/rabbitmq/amqp_connection.c",
        "src/rabbitmq/amqp_consumer.c",
        "src/rabbitmq/amqp_framing.c",
        "src/rabbitmq/amqp_mem.c",
        "src/rabbitmq/amqp_openssl.c",
        "src/rabbitmq/amqp_openssl_bio.c",
        "src/rabbitmq/amqp_socket.c",
        "src/rabbitmq/amqp_table.c",
        "src/rabbitmq/amqp_tcp_socket.c",
        "src/rabbitmq/amqp_time.c",
        "src/rabbitmq/amqp_url.c",
    };

    const lib_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .strip = strip_binaries,
        .link_libc = true,
        .sanitize_c = sanitize_c,
    });
    lib_module.addIncludePath(b.path("include"));
    lib_module.addCSourceFile(.{ .file = b.path("src/ws_client.c"), .flags = c_flags });
    if (use_mimalloc) {
        lib_module.addCSourceFile(.{ .file = b.path("src/mimalloc_override.c"), .flags = c_flags });
    }

    const lib = b.addLibrary(.{
        .name = "websocket_client",
        .linkage = .static,
        .root_module = lib_module,
    });
    b.installArtifact(lib);

    const monitor_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .strip = strip_binaries,
        .link_libc = true,
        .sanitize_c = sanitize_c,
    });
    monitor_module.addIncludePath(b.path("include"));
    monitor_module.addCSourceFile(.{ .file = b.path("src/parg.c"), .flags = c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/toml.c"), .flags = c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/ulog.c"), .flags = ulog_c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/parson.c"), .flags = c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/rabbitmq_publisher.c"), .flags = c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/subscriber.c"), .flags = c_flags });
    monitor_module.addCSourceFile(.{ .file = b.path("src/main.c"), .flags = c_flags });
    if (use_mimalloc) {
        monitor_module.addCSourceFile(.{ .file = b.path("src/mimalloc_override.c"), .flags = c_flags });
    }
    for (hiredis_files) |file| {
        monitor_module.addCSourceFile(.{ .file = b.path(file), .flags = hiredis_c_flags });
    }
    for (rabbitmq_files) |file| {
        monitor_module.addCSourceFile(.{ .file = b.path(file), .flags = rabbitmq_c_flags });
    }

    const monitor = b.addExecutable(.{
        .name = "eth_mempool_monitor",
        .root_module = monitor_module,
    });
    monitor.linkLibrary(lib);
    monitor.linkSystemLibrary("wolfssl");
    if (use_mimalloc) {
        monitor.linkSystemLibrary("mimalloc");
    }
    monitor.linkLibC();

    b.installArtifact(monitor);

    const run_monitor = b.addRunArtifact(monitor);
    if (b.args) |args| {
        run_monitor.addArgs(args);
    }

    const run_step = b.step("run-example", "Run ETH mempool monitor");
    run_step.dependOn(&run_monitor.step);

    const rabbitmq_console_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .strip = strip_binaries,
        .link_libc = true,
        .sanitize_c = sanitize_c,
    });
    rabbitmq_console_module.addIncludePath(b.path("include"));
    rabbitmq_console_module.addCSourceFile(.{ .file = b.path("src/toml.c"), .flags = c_flags });
    rabbitmq_console_module.addCSourceFile(.{ .file = b.path("src/parson.c"), .flags = c_flags });
    rabbitmq_console_module.addCSourceFile(.{ .file = b.path("src/ulog.c"), .flags = ulog_c_flags });
    rabbitmq_console_module.addCSourceFile(.{ .file = b.path("src/rabbitmq_tx_console.c"), .flags = c_flags });
    if (use_mimalloc) {
        rabbitmq_console_module.addCSourceFile(.{ .file = b.path("src/mimalloc_override.c"), .flags = c_flags });
    }
    for (rabbitmq_files) |file| {
        rabbitmq_console_module.addCSourceFile(.{ .file = b.path(file), .flags = rabbitmq_c_flags });
    }

    const rabbitmq_console = b.addExecutable(.{
        .name = "rabbitmq_tx_console",
        .root_module = rabbitmq_console_module,
    });
    rabbitmq_console.linkSystemLibrary("wolfssl");
    if (use_mimalloc) {
        rabbitmq_console.linkSystemLibrary("mimalloc");
    }
    rabbitmq_console.linkLibC();
    b.installArtifact(rabbitmq_console);

    const run_rabbitmq_console = b.addRunArtifact(rabbitmq_console);
    if (b.args) |args| {
        run_rabbitmq_console.addArgs(args);
    }

    const run_rabbitmq_console_step = b.step(
        "run-rabbitmq-console",
        "Run RabbitMQ monitored transaction console",
    );
    run_rabbitmq_console_step.dependOn(&run_rabbitmq_console.step);

    const rpc_control_module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .strip = strip_binaries,
        .link_libc = true,
        .sanitize_c = sanitize_c,
    });
    rpc_control_module.addIncludePath(b.path("include"));
    rpc_control_module.addCSourceFile(.{ .file = b.path("src/toml.c"), .flags = c_flags });
    rpc_control_module.addCSourceFile(.{ .file = b.path("src/ulog.c"), .flags = ulog_c_flags });
    rpc_control_module.addCSourceFile(.{ .file = b.path("src/rpc_control.c"), .flags = jsonrpc_c_flags });
    if (use_mimalloc) {
        rpc_control_module.addCSourceFile(.{ .file = b.path("src/mimalloc_override.c"), .flags = c_flags });
    }
    for (jsonrpc_files) |file| {
        rpc_control_module.addCSourceFile(.{ .file = b.path(file), .flags = jsonrpc_c_flags });
    }
    for (hiredis_files) |file| {
        rpc_control_module.addCSourceFile(.{ .file = b.path(file), .flags = hiredis_c_flags });
    }

    const rpc_control = b.addExecutable(.{
        .name = "rpc_control",
        .root_module = rpc_control_module,
    });
    rpc_control.linkSystemLibrary("uv");
    if (use_mimalloc) {
        rpc_control.linkSystemLibrary("mimalloc");
    }
    rpc_control.linkLibC();
    b.installArtifact(rpc_control);

    const run_rpc_control = b.addRunArtifact(rpc_control);
    if (b.args) |args| {
        run_rpc_control.addArgs(args);
    }

    const run_rpc_control_step = b.step("run-rpc-control", "Run JSON-RPC control server");
    run_rpc_control_step.dependOn(&run_rpc_control.step);

    const valgrind_rpc_control_cmd = b.addSystemCommand(&.{
        "valgrind",
        "--tool=massif",
        "--stacks=yes",
    });
    valgrind_rpc_control_cmd.addArtifactArg(rpc_control);
    if (b.args) |args| {
        valgrind_rpc_control_cmd.addArgs(args);
    }

    const valgrind_rpc_control_step = b.step(
        "valgrind-rpc-control",
        "Run JSON-RPC control server under Valgrind Massif",
    );
    valgrind_rpc_control_step.dependOn(&valgrind_rpc_control_cmd.step);
}
