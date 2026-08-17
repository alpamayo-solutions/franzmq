"""Unit tests for the pinning-model TLS context."""
import ssl

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key

from franzmq.pinned_tls import load_identity_key, self_signed_cert_pem, self_signed_context


def write_ed25519_key(tmp_path, name="m1.key"):
    key = Ed25519PrivateKey.generate()
    path = tmp_path / name
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return path, key


def test_context_presents_a_cert_and_does_not_verify_the_broker(tmp_path):
    path, _ = write_ed25519_key(tmp_path)
    ctx = self_signed_context(path, "m1")

    # Trust runs the other way: the broker pins our public key; we have no CA.
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False
    assert ctx.get_ca_certs() == []
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_3


def test_certificate_carries_the_identity_and_the_keys_public_half(tmp_path):
    path, key = write_ed25519_key(tmp_path)

    cert = x509.load_pem_x509_certificate(self_signed_cert_pem(key, "m1"))

    assert cert.subject.rfc4514_string() == "CN=m1"
    assert cert.issuer == cert.subject  # self-signed: the cert is a key container
    assert cert.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    ) == key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )


def test_a_non_ed25519_key_is_refused_by_name(tmp_path):
    path = tmp_path / "rsa.key"
    path.write_bytes(
        generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    with pytest.raises(ValueError, match="ed25519"):
        load_identity_key(path)


def test_a_file_that_is_not_a_key_says_so(tmp_path):
    path = tmp_path / "junk.key"
    path.write_bytes(b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n")
    with pytest.raises(ValueError, match="PEM private key"):
        load_identity_key(path)
