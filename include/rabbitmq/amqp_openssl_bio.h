// Copyright 2007 - 2021, Alan Antonuk and the rabbitmq-c contributors.
// SPDX-License-Identifier: mit

#ifndef AMQP_OPENSSL_BIO
#define AMQP_OPENSSL_BIO

// Use OpenSSL v1.1.1 API.
#define OPENSSL_API_COMPAT 10101

#include <wolfssl/openssl/bio.h>
#include <wolfssl/openssl/ssl.h>
#include <wolfssl/options.h>

int amqp_openssl_bio_init();

void amqp_openssl_bio_destroy();

typedef BIO_METHOD *BIO_METHOD_PTR;

BIO_METHOD_PTR amqp_openssl_bio();

#endif /* ifndef AMQP_OPENSSL_BIO */
