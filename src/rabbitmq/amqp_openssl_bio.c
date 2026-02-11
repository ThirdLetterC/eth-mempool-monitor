// Copyright 2007 - 2021, Alan Antonuk and the rabbitmq-c contributors.
// SPDX-License-Identifier: mit

#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <wolfssl/options.h>
#include <wolfssl/openssl/bio.h>
#include <wolfssl/openssl/ssl.h>

#include "rabbitmq//amqp_openssl_bio.h"
#include "rabbitmq//amqp_socket.h"
#include "ulog/ulog.h"

#if defined(MSG_NOSIGNAL) && !defined(HAVE_WOLFSSL_SSL_H)
#define AMQP_USE_AMQP_BIO
#endif

static int amqp_ssl_bio_initialized = 0;

#ifdef AMQP_USE_AMQP_BIO

static BIO_METHOD *amqp_bio_method;

static int amqp_openssl_bio_should_retry(int res) {
  if (res == -1) {
    int err = amqp_os_socket_error();
    if (
#ifdef EWOULDBLOCK
        err == EWOULDBLOCK ||
#endif
#ifdef ENOTCONN
        err == ENOTCONN ||
#endif
#ifdef EINTR
        err == EINTR ||
#endif
#ifdef EAGAIN
        err == EAGAIN ||
#endif
#ifdef EPROTO
        err == EPROTO ||
#endif
#ifdef EINPROGRESS
        err == EINPROGRESS ||
#endif
#ifdef EALREADY
        err == EALREADY ||
#endif
        0) {
      ulog_trace("[rabbitmq-ssl-bio] retryable socket error=%d", err);
      return 1;
    }
  }
  return 0;
}

static int amqp_openssl_bio_write(BIO *b, const char *in, int inl) {
  int flags = 0;
  int fd;
  int res;

#ifdef MSG_NOSIGNAL
  flags |= MSG_NOSIGNAL;
#endif

  BIO_get_fd(b, &fd);
  ulog_trace("[rabbitmq-ssl-bio] write fd=%d bytes=%d", fd, inl);
  res = send(fd, in, inl, flags);

  BIO_clear_retry_flags(b);
  if (res <= 0 && amqp_openssl_bio_should_retry(res)) {
    BIO_set_retry_write(b);
  }
  if (res <= 0) {
    ulog_debug("[rabbitmq-ssl-bio] write failed fd=%d result=%d", fd, res);
  } else {
    ulog_trace("[rabbitmq-ssl-bio] write sent=%d fd=%d", res, fd);
  }

  return res;
}

static int amqp_openssl_bio_read(BIO *b, char *out, int outl) {
  int flags = 0;
  int fd;
  int res;

#ifdef MSG_NOSIGNAL
  flags |= MSG_NOSIGNAL;
#endif

  BIO_get_fd(b, &fd);
  ulog_trace("[rabbitmq-ssl-bio] read fd=%d bytes=%d", fd, outl);
  res = recv(fd, out, outl, flags);

  BIO_clear_retry_flags(b);
  if (res <= 0 && amqp_openssl_bio_should_retry(res)) {
    BIO_set_retry_read(b);
  }
  if (res <= 0) {
    ulog_debug("[rabbitmq-ssl-bio] read failed fd=%d result=%d", fd, res);
  } else {
    ulog_trace("[rabbitmq-ssl-bio] read received=%d fd=%d", res, fd);
  }

  return res;
}
#endif /* AMQP_USE_AMQP_BIO */

int amqp_openssl_bio_init() {
  assert(!amqp_ssl_bio_initialized);
  ulog_trace("[rabbitmq-ssl-bio] init");
#ifdef AMQP_USE_AMQP_BIO
  if (!(amqp_bio_method = BIO_meth_new(BIO_TYPE_SOCKET, "amqp_bio_method"))) {
    ulog_debug("[rabbitmq-ssl-bio] BIO_meth_new failed");
    return AMQP_STATUS_NO_MEMORY;
  }
#ifdef OPENSSL_IS_BORINGSSL
  BIO_meth_set_create(amqp_bio_method, BIO_s_socket()->create);
  BIO_meth_set_destroy(amqp_bio_method, BIO_s_socket()->destroy);
  BIO_meth_set_ctrl(amqp_bio_method, BIO_s_socket()->ctrl);
  BIO_meth_set_read(amqp_bio_method, BIO_s_socket()->bread);
  BIO_meth_set_write(amqp_bio_method, BIO_s_socket()->bwrite);
  BIO_meth_set_gets(amqp_bio_method, BIO_s_socket()->bgets);
  BIO_meth_set_puts(amqp_bio_method, BIO_s_socket()->bputs);
#else
  BIO_meth_set_create(amqp_bio_method, BIO_meth_get_create(BIO_s_socket()));
  BIO_meth_set_destroy(amqp_bio_method, BIO_meth_get_destroy(BIO_s_socket()));
  BIO_meth_set_ctrl(amqp_bio_method, BIO_meth_get_ctrl(BIO_s_socket()));
  BIO_meth_set_callback_ctrl(amqp_bio_method,
                             BIO_meth_get_callback_ctrl(BIO_s_socket()));
  BIO_meth_set_read(amqp_bio_method, BIO_meth_get_read(BIO_s_socket()));
  BIO_meth_set_write(amqp_bio_method, BIO_meth_get_write(BIO_s_socket()));
  BIO_meth_set_gets(amqp_bio_method, BIO_meth_get_gets(BIO_s_socket()));
  BIO_meth_set_puts(amqp_bio_method, BIO_meth_get_puts(BIO_s_socket()));
#endif

  BIO_meth_set_write(amqp_bio_method, amqp_openssl_bio_write);
  BIO_meth_set_read(amqp_bio_method, amqp_openssl_bio_read);
#endif

  amqp_ssl_bio_initialized = 1;
  ulog_debug("[rabbitmq-ssl-bio] init complete");
  return AMQP_STATUS_OK;
}

void amqp_openssl_bio_destroy() {
  assert(amqp_ssl_bio_initialized);
  ulog_trace("[rabbitmq-ssl-bio] destroy");
#ifdef AMQP_USE_AMQP_BIO
  BIO_meth_free(amqp_bio_method);
  amqp_bio_method = nullptr;
#endif
  amqp_ssl_bio_initialized = 0;
}

BIO_METHOD_PTR amqp_openssl_bio() {
  assert(amqp_ssl_bio_initialized);
#ifdef AMQP_USE_AMQP_BIO
  return amqp_bio_method;
#else
  return BIO_s_socket();
#endif
}
