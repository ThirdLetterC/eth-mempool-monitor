const std = @import("std");

const posix_feature_flag = "-D_POSIX_C_SOURCE=200809L";

const strict_c_flags = [_][]const u8{
    "-std=c23",
    "-Wall",
    "-Wextra",
    "-Wpedantic",
    "-Werror",
};

const conversion_c_flags = [_][]const u8{
    "-Wconversion",
    "-Wsign-conversion",
    "-Wenum-conversion",
    "-Wimplicit-int-conversion",
};

const first_party_c_flags = strict_c_flags ++ conversion_c_flags;

const posix_c_flags = [_][]const u8{
    "-std=c23",
    "-D_DEFAULT_SOURCE",
    posix_feature_flag,
};

const hardening_c_flags = [_][]const u8{
    "-fstack-protector-strong",
    "-D_FORTIFY_SOURCE=3",
    "-fPIE",
};

const jsonrpc_files = [_][]const u8{
    "src/jsonrpc/arena.c",
    "src/jsonrpc/jsonrpc.c",
    "src/jsonrpc/parson.c",
    "src/jsonrpc/server.c",
};

const hiredis_files = [_][]const u8{
    "src/hiredis/alloc.c",
    "src/hiredis/async.c",
    "src/hiredis/dict.c",
    "src/hiredis/hiredis.c",
    "src/hiredis/net.c",
    "src/hiredis/read.c",
    "src/hiredis/sds.c",
};

const rabbitmq_files = [_][]const u8{
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

const curl_files = [_][]const u8{
    "altsvc.c",
    "amigaos.c",
    "api.c",
    "bufq.c",
    "bufref.c",
    "cf-h1-proxy.c",
    "cf-h2-proxy.c",
    "cf-haproxy.c",
    "cf-https-connect.c",
    "cf-ip-happy.c",
    "cf-recvbuf.c",
    "cf-setup.c",
    "cf-socket.c",
    "cfilters.c",
    "conncache.c",
    "connect.c",
    "content_encoding.c",
    "cookie.c",
    "creds.c",
    "cshutdn.c",
    "curl_addrinfo.c",
    "curl_ed25519.c",
    "curl_endian.c",
    "curl_fnmatch.c",
    "curl_fopen.c",
    "curl_get_line.c",
    "curl_gethostname.c",
    "curl_gssapi.c",
    "curl_memrchr.c",
    "curl_ntlm_core.c",
    "curl_range.c",
    "curl_sasl.c",
    "curl_sha512_256.c",
    "curl_share.c",
    "curl_sspi.c",
    "curl_threads.c",
    "curl_trc.c",
    "curlx/base64.c",
    "curlx/basename.c",
    "curlx/dynbuf.c",
    "curlx/fopen.c",
    "curlx/inet_ntop.c",
    "curlx/inet_pton.c",
    "curlx/multibyte.c",
    "curlx/nonblock.c",
    "curlx/snprintf.c",
    "curlx/strcopy.c",
    "curlx/strdup.c",
    "curlx/strerr.c",
    "curlx/strparse.c",
    "curlx/timediff.c",
    "curlx/timeval.c",
    "curlx/version_win32.c",
    "curlx/wait.c",
    "curlx/warnless.c",
    "curlx/winapi.c",
    "cw-out.c",
    "cw-pause.c",
    "dict.c",
    "dllmain.c",
    "dynhds.c",
    "easy.c",
    "easygetopt.c",
    "easyoptions.c",
    "escape.c",
    "fake_addrinfo.c",
    "file.c",
    "fileinfo.c",
    "formdata.c",
    "ftp.c",
    "ftplistparser.c",
    "getenv.c",
    "getinfo.c",
    "gopher.c",
    "hash.c",
    "headers.c",
    "hmac.c",
    "hsts.c",
    "http.c",
    "http1.c",
    "http2.c",
    "http_aws_sigv4.c",
    "http_chunks.c",
    "http_digest.c",
    "http_httpsig.c",
    "http_negotiate.c",
    "http_ntlm.c",
    "http_proxy.c",
    "idn.c",
    "if2ip.c",
    "imap.c",
    "ldap.c",
    "llist.c",
    "macos.c",
    "md4.c",
    "md5.c",
    "memdebug.c",
    "mime.c",
    "mprintf.c",
    "mqtt.c",
    "multi.c",
    "multi_ev.c",
    "multi_ntfy.c",
    "netrc.c",
    "openldap.c",
    "parsedate.c",
    "peer.c",
    "pingpong.c",
    "pop3.c",
    "progress.c",
    "protocol.c",
    "proxy.c",
    "psl.c",
    "rand.c",
    "ratelimit.c",
    "request.c",
    "rtsp.c",
    "select.c",
    "sendf.c",
    "setopt.c",
    "sha256.c",
    "slist.c",
    "smb.c",
    "smtp.c",
    "socketpair.c",
    "socks.c",
    "socks_gssapi.c",
    "socks_sspi.c",
    "splay.c",
    "strcase.c",
    "strequal.c",
    "strerror.c",
    "system_win32.c",
    "telnet.c",
    "tftp.c",
    "thrdpool.c",
    "thrdqueue.c",
    "transfer.c",
    "uint-bset.c",
    "uint-hash.c",
    "uint-hashset.c",
    "uint-spbset.c",
    "uint-table.c",
    "url.c",
    "urlapi.c",
    "vauth/cleartext.c",
    "vauth/cram.c",
    "vauth/digest.c",
    "vauth/digest_sspi.c",
    "vauth/gsasl.c",
    "vauth/krb5_gssapi.c",
    "vauth/krb5_sspi.c",
    "vauth/ntlm.c",
    "vauth/ntlm_sspi.c",
    "vauth/oauth2.c",
    "vauth/spnego_gssapi.c",
    "vauth/spnego_sspi.c",
    "vauth/vauth.c",
    "vdns/asyn-ares.c",
    "vdns/asyn-base.c",
    "vdns/asyn-thrdd.c",
    "vdns/cf-dns.c",
    "vdns/dnscache.c",
    "vdns/doh.c",
    "vdns/hostip.c",
    "vdns/hostip4.c",
    "vdns/hostip6.c",
    "vdns/httpsrr.c",
    "version.c",
    "vquic/capsule.c",
    "vquic/cf-capsule.c",
    "vquic/cf-ngtcp2-cmn.c",
    "vquic/cf-ngtcp2-proxy.c",
    "vquic/cf-ngtcp2.c",
    "vquic/cf-quiche.c",
    "vquic/vquic-tls.c",
    "vquic/vquic.c",
    "vssh/libssh.c",
    "vssh/libssh2.c",
    "vssh/vssh.c",
    "vtls/apple.c",
    "vtls/cipher_suite.c",
    "vtls/gtls.c",
    "vtls/hostcheck.c",
    "vtls/keylog.c",
    "vtls/mbedtls.c",
    "vtls/openssl.c",
    "vtls/rustls.c",
    "vtls/schannel.c",
    "vtls/schannel_verify.c",
    "vtls/vtls.c",
    "vtls/vtls_config.c",
    "vtls/vtls_scache.c",
    "vtls/vtls_spack.c",
    "vtls/wolfssl.c",
    "vtls/x509asn1.c",
    "ws.c",
};

fn makeCFlags(
    b: *std.Build,
    base_flags: []const []const u8,
    component_flags: []const []const u8,
    enable_hardening: bool,
) []const []const u8 {
    const hardening_count = if (enable_hardening) hardening_c_flags.len else 0;
    const flags = b.allocator.alloc(
        []const u8,
        base_flags.len + component_flags.len + hardening_count,
    ) catch @panic("failed to allocate C compiler flags");

    var index: usize = 0;
    for (base_flags) |flag| {
        flags[index] = flag;
        index += 1;
    }
    for (component_flags) |flag| {
        flags[index] = flag;
        index += 1;
    }
    if (enable_hardening) {
        for (hardening_c_flags) |flag| {
            flags[index] = flag;
            index += 1;
        }
    }

    return flags;
}

fn createCModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    strip_binaries: bool,
    sanitize_c: std.zig.SanitizeC,
) *std.Build.Module {
    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .strip = strip_binaries,
        .link_libc = true,
        .sanitize_c = sanitize_c,
    });
    module.addIncludePath(b.path("include"));
    return module;
}

fn createCurlLibrary(
    b: *std.Build,
    dependency: *std.Build.Dependency,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    if (target.result.os.tag != .linux) {
        @panic("the first-party libcurl configuration currently supports Linux targets only");
    }

    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    module.addIncludePath(b.path("include"));
    module.addIncludePath(dependency.path("include"));
    module.addIncludePath(dependency.path("lib"));
    module.addCMacro("BUILDING_LIBCURL", "1");
    module.addCMacro("CURL_STATICLIB", "1");
    module.addCMacro("CURL_HIDDEN_SYMBOLS", "1");
    module.addCMacro("HAVE_CONFIG_H", "1");
    module.addCSourceFiles(.{
        .root = dependency.path("lib"),
        .files = &curl_files,
        .flags = &.{
            "-std=c23",
            "-D_GNU_SOURCE=1",
            "-fvisibility=hidden",
        },
    });

    return b.addLibrary(.{
        .name = "curl",
        .linkage = .static,
        .root_module = module,
    });
}

fn addCFiles(
    b: *std.Build,
    module: *std.Build.Module,
    files: []const []const u8,
    flags: []const []const u8,
) void {
    for (files) |file| {
        module.addCSourceFile(.{ .file = b.path(file), .flags = flags });
    }
}

fn addExecutable(
    b: *std.Build,
    name: []const u8,
    module: *std.Build.Module,
    enable_hardening: bool,
) *std.Build.Step.Compile {
    const executable = b.addExecutable(.{
        .name = name,
        .root_module = module,
    });
    if (enable_hardening) {
        executable.pie = true;
        executable.link_z_relro = true;
        executable.link_z_lazy = false;
    }
    b.installArtifact(executable);
    return executable;
}

fn linkOptionalLibrary(
    module: *std.Build.Module,
    library: ?*std.Build.Step.Compile,
) void {
    if (library) |enabled_library| {
        module.linkLibrary(enabled_library);
    }
}

fn linkStaticSystemLibrary(module: *std.Build.Module, name: []const u8) void {
    module.linkSystemLibrary(name, .{
        .preferred_link_mode = .static,
        .search_strategy = .no_fallback,
    });
}

fn addRunStep(
    b: *std.Build,
    executable: *std.Build.Step.Compile,
    name: []const u8,
    description: []const u8,
) void {
    const run_artifact = b.addRunArtifact(executable);
    if (b.args) |args| {
        run_artifact.addArgs(args);
    }

    const run_step = b.step(name, description);
    run_step.dependOn(&run_artifact.step);
}

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

    const optimize = if (release_mode) .ReleaseFast else b.standardOptimizeOption(.{});
    const default_target_query: std.Target.Query = if (optimize == .Debug)
        .{}
    else
        .{ .cpu_model = .baseline };
    const base_target_query = b.standardTargetOptionsQueryOnly(.{
        .default_target = default_target_query,
    });
    const base_target = b.resolveTargetQuery(base_target_query);

    var valgrind_target_query = base_target_query;
    valgrind_target_query.cpu_model = .baseline;
    valgrind_target_query.cpu_features_add = .empty;
    valgrind_target_query.cpu_features_sub = .empty;
    const valgrind_target = b.resolveTargetQuery(valgrind_target_query);

    const target = if (force_valgrind) valgrind_target else base_target;
    const use_sanitizers = enable_sanitizers and optimize == .Debug and target.result.os.tag != .windows;
    const sanitize_c = if (use_sanitizers) std.zig.SanitizeC.full else std.zig.SanitizeC.off;
    const enable_hardening = target.result.os.tag == .linux;
    const mimalloc_dependency = if (use_mimalloc)
        b.lazyDependency("mimalloc", .{}) orelse return
    else
        null;
    const curl_dependency = b.dependency("curl", .{});
    const curl_library = createCurlLibrary(
        b,
        curl_dependency,
        target,
        .ReleaseFast,
    );

    const no_component_flags = &[_][]const u8{};
    const c_component_flags = if (use_mimalloc)
        &[_][]const u8{ "-DWC_NO_HARDEN", "-DUSE_MIMALLOC=1" }
    else
        &[_][]const u8{"-DWC_NO_HARDEN"};
    const project_posix_component_flags = if (use_mimalloc)
        &[_][]const u8{
            "-DWC_NO_HARDEN",
            "-DUSE_MIMALLOC=1",
            posix_feature_flag,
        }
    else
        &[_][]const u8{
            "-DWC_NO_HARDEN",
            posix_feature_flag,
        };
    const hiredis_component_flags = if (use_mimalloc)
        &[_][]const u8{"-DHIREDIS_USE_MIMALLOC=1"}
    else
        no_component_flags;
    const jsonrpc_component_flags = if (use_mimalloc)
        &[_][]const u8{"-DUSE_MIMALLOC=1"}
    else
        no_component_flags;

    const c_flags = makeCFlags(b, &strict_c_flags, c_component_flags, enable_hardening);
    const project_c_flags = makeCFlags(
        b,
        &first_party_c_flags,
        c_component_flags,
        enable_hardening,
    );
    const project_posix_c_flags = makeCFlags(
        b,
        &first_party_c_flags,
        project_posix_component_flags,
        enable_hardening,
    );
    const ulog_c_flags = makeCFlags(
        b,
        &strict_c_flags,
        &.{"-DULOG_BUILD_DYNAMIC_CONFIG=1"},
        enable_hardening,
    );
    const hiredis_c_flags = makeCFlags(
        b,
        &posix_c_flags,
        hiredis_component_flags,
        enable_hardening,
    );
    const rabbitmq_c_flags = makeCFlags(
        b,
        &posix_c_flags,
        &.{ "-DHAVE_POLL", "-DWC_NO_HARDEN" },
        enable_hardening,
    );
    const jsonrpc_c_flags = makeCFlags(
        b,
        &posix_c_flags,
        jsonrpc_component_flags,
        enable_hardening,
    );

    var mimalloc_library: ?*std.Build.Step.Compile = null;
    if (mimalloc_dependency) |dependency| {
        const mimalloc_c_flags = if (optimize == .Debug)
            &[_][]const u8{
                "-std=c23",
                "-DMI_MALLOC_OVERRIDE=1",
                "-DMI_STATIC_LIB=1",
            }
        else
            &[_][]const u8{
                "-std=c23",
                "-DMI_BUILD_RELEASE=1",
                "-DMI_MALLOC_OVERRIDE=1",
                "-DMI_STATIC_LIB=1",
            };
        const mimalloc_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .sanitize_c = sanitize_c,
        });
        mimalloc_module.addIncludePath(dependency.path("include"));
        mimalloc_module.addCSourceFile(.{
            .file = dependency.path("src/static.c"),
            .flags = mimalloc_c_flags,
        });
        mimalloc_library = b.addLibrary(.{
            .name = "mimalloc",
            .linkage = .static,
            .root_module = mimalloc_module,
        });
    }

    const websocket_module = createCModule(b, target, optimize, strip_binaries, sanitize_c);
    addCFiles(b, websocket_module, &.{"src/ws_frame.c"}, project_c_flags);
    addCFiles(b, websocket_module, &.{
        "src/ws_client.c",
        "src/ws_handshake.c",
        "src/ws_transport.c",
    }, project_posix_c_flags);
    const websocket_library = b.addLibrary(.{
        .name = "websocket_client",
        .linkage = .static,
        .root_module = websocket_module,
    });
    b.installArtifact(websocket_library);

    const type_test_module = createCModule(
        b,
        target,
        .Debug,
        false,
        sanitize_c,
    );
    addCFiles(
        b,
        type_test_module,
        &.{"tests/type_safety_test.c"},
        project_c_flags,
    );
    addCFiles(b, type_test_module, &.{"src/ulog.c"}, ulog_c_flags);
    type_test_module.linkLibrary(websocket_library);
    linkStaticSystemLibrary(type_test_module, "wolfssl");
    const type_test = b.addExecutable(.{
        .name = "type_safety_test",
        .root_module = type_test_module,
    });
    const run_type_test = b.addRunArtifact(type_test);
    const test_step = b.step("test", "Run first-party C type-safety tests");
    test_step.dependOn(&run_type_test.step);

    const http_config_test_module = createCModule(
        b,
        target,
        .Debug,
        false,
        sanitize_c,
    );
    addCFiles(b, http_config_test_module, &.{"src/toml.c"}, c_flags);
    addCFiles(b, http_config_test_module, &.{
        "tests/http_transmitter_config_test.c",
        "src/http_transmitter_config.c",
    }, project_posix_c_flags);
    addCFiles(b, http_config_test_module, &.{"src/ulog.c"}, ulog_c_flags);
    http_config_test_module.addIncludePath(curl_dependency.path("include"));
    http_config_test_module.linkLibrary(curl_library);
    linkStaticSystemLibrary(http_config_test_module, "wolfssl");
    const http_config_test = b.addExecutable(.{
        .name = "http_transmitter_config_test",
        .root_module = http_config_test_module,
    });
    const run_http_config_test = b.addRunArtifact(http_config_test);
    test_step.dependOn(&run_http_config_test.step);

    const http_webhook_test_module = createCModule(
        b,
        target,
        .Debug,
        false,
        sanitize_c,
    );
    addCFiles(b, http_webhook_test_module, &.{"src/parson.c"}, c_flags);
    addCFiles(b, http_webhook_test_module, &.{
        "tests/http_transmitter_webhook_test.c",
        "src/http_transmitter_webhook.c",
    }, project_posix_c_flags);
    addCFiles(b, http_webhook_test_module, &.{"src/ulog.c"}, ulog_c_flags);
    http_webhook_test_module.addIncludePath(curl_dependency.path("include"));
    http_webhook_test_module.linkLibrary(curl_library);
    linkStaticSystemLibrary(http_webhook_test_module, "wolfssl");
    const http_webhook_test = b.addExecutable(.{
        .name = "http_transmitter_webhook_test",
        .root_module = http_webhook_test_module,
    });
    const run_http_webhook_test = b.addRunArtifact(http_webhook_test);
    test_step.dependOn(&run_http_webhook_test.step);

    const monitor_module = createCModule(b, target, optimize, strip_binaries, sanitize_c);
    addCFiles(b, monitor_module, &.{
        "src/parg.c",
        "src/toml.c",
        "src/parson.c",
    }, c_flags);
    addCFiles(b, monitor_module, &.{
        "src/subscriber.c",
        "src/subscriber_message.c",
        "src/main.c",
    }, project_c_flags);
    addCFiles(b, monitor_module, &.{
        "src/monitor_config.c",
        "src/monitor_runtime.c",
        "src/rabbitmq_publisher.c",
        "src/rabbitmq_publisher_connection.c",
        "src/rabbitmq_publisher_replay.c",
        "src/rabbitmq_publisher_worker.c",
    }, project_posix_c_flags);
    addCFiles(b, monitor_module, &.{"src/ulog.c"}, ulog_c_flags);
    addCFiles(b, monitor_module, &hiredis_files, hiredis_c_flags);
    addCFiles(b, monitor_module, &rabbitmq_files, rabbitmq_c_flags);
    if (mimalloc_dependency) |dependency| {
        monitor_module.addIncludePath(dependency.path("include"));
    }
    monitor_module.linkLibrary(websocket_library);
    linkStaticSystemLibrary(monitor_module, "uv");
    linkStaticSystemLibrary(monitor_module, "wolfssl");
    linkOptionalLibrary(monitor_module, mimalloc_library);

    const monitor = addExecutable(b, "eth_mempool_monitor", monitor_module, enable_hardening);
    addRunStep(b, monitor, "run-example", "Run ETH mempool monitor");

    const rabbitmq_console_module = createCModule(b, target, optimize, strip_binaries, sanitize_c);
    addCFiles(b, rabbitmq_console_module, &.{
        "src/toml.c",
        "src/parson.c",
    }, c_flags);
    addCFiles(b, rabbitmq_console_module, &.{
        "src/rabbitmq_tx_console_format.c",
    }, project_c_flags);
    addCFiles(b, rabbitmq_console_module, &.{
        "src/rabbitmq_tx_console.c",
        "src/rabbitmq_tx_console_config.c",
        "src/rabbitmq_tx_console_consumer.c",
    }, project_posix_c_flags);
    addCFiles(b, rabbitmq_console_module, &.{"src/ulog.c"}, ulog_c_flags);
    addCFiles(b, rabbitmq_console_module, &rabbitmq_files, rabbitmq_c_flags);
    if (mimalloc_dependency) |dependency| {
        rabbitmq_console_module.addIncludePath(dependency.path("include"));
    }
    linkStaticSystemLibrary(rabbitmq_console_module, "wolfssl");
    linkOptionalLibrary(rabbitmq_console_module, mimalloc_library);

    const rabbitmq_console = addExecutable(
        b,
        "rabbitmq_tx_console",
        rabbitmq_console_module,
        enable_hardening,
    );
    addRunStep(
        b,
        rabbitmq_console,
        "run-rabbitmq-console",
        "Run RabbitMQ monitored transaction console",
    );

    const http_transmitter_module = createCModule(
        b,
        target,
        optimize,
        strip_binaries,
        sanitize_c,
    );
    addCFiles(b, http_transmitter_module, &.{
        "src/toml.c",
        "src/parson.c",
    }, c_flags);
    addCFiles(b, http_transmitter_module, &.{
        "src/http_transmitter.c",
        "src/http_transmitter_config.c",
        "src/http_transmitter_consumer.c",
        "src/http_transmitter_webhook.c",
    }, project_posix_c_flags);
    addCFiles(b, http_transmitter_module, &.{"src/ulog.c"}, ulog_c_flags);
    addCFiles(b, http_transmitter_module, &rabbitmq_files, rabbitmq_c_flags);
    if (mimalloc_dependency) |dependency| {
        http_transmitter_module.addIncludePath(dependency.path("include"));
    }
    http_transmitter_module.addIncludePath(curl_dependency.path("include"));
    http_transmitter_module.linkLibrary(curl_library);
    linkStaticSystemLibrary(http_transmitter_module, "wolfssl");
    linkOptionalLibrary(http_transmitter_module, mimalloc_library);

    const http_transmitter = addExecutable(
        b,
        "http_transmitter",
        http_transmitter_module,
        enable_hardening,
    );
    addRunStep(
        b,
        http_transmitter,
        "run-http-transmitter",
        "Run RabbitMQ-to-webhook transaction transmitter",
    );

    const rpc_control_module = createCModule(b, target, optimize, strip_binaries, sanitize_c);
    addCFiles(b, rpc_control_module, &.{"src/toml.c"}, c_flags);
    addCFiles(b, rpc_control_module, &.{"src/ulog.c"}, ulog_c_flags);
    addCFiles(b, rpc_control_module, &.{
        "src/rpc_control_config.c",
        "src/rpc_control_service.c",
        "src/rpc_control.c",
    }, project_posix_c_flags);
    addCFiles(b, rpc_control_module, &jsonrpc_files, jsonrpc_c_flags);
    addCFiles(b, rpc_control_module, &hiredis_files, hiredis_c_flags);
    if (mimalloc_dependency) |dependency| {
        rpc_control_module.addIncludePath(dependency.path("include"));
    }
    linkStaticSystemLibrary(rpc_control_module, "uv");
    linkOptionalLibrary(rpc_control_module, mimalloc_library);

    const rpc_control = addExecutable(b, "rpc_control", rpc_control_module, enable_hardening);
    addRunStep(b, rpc_control, "run-rpc-control", "Run JSON-RPC control server");

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
