"""TLS for the pinning model: the key is the credential, there is no CA.

The client presents a certificate it signed with its own key. That certificate
is a container for the public key and proves nothing by itself — the broker
looks at the public key inside and checks it against the identities enrolled
there, and an unknown key is refused. Nothing validates the broker in return,
because no authority exists to validate it against: trust runs the other way.

This mirrors the broker's own reference client, which loads its key, builds the
certificate in memory and connects. Keeping it here rather than in each service
means the trust model exists once, in the library whose job is talking to that
broker.
"""
import datetime
import pathlib
import ssl
import tempfile

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.x509.oid import NameOID

#: Matches the reference implementation's certificate lifetime.
CERT_VALIDITY = datetime.timedelta(days=3650)


def load_identity_key(key_path: pathlib.Path) -> Ed25519PrivateKey:
    """Load the ed25519 identity key, or say precisely what is wrong with it."""
    try:
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    except ValueError as exc:
        raise ValueError(f"{key_path}: not a readable PEM private key ({exc})") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError(
            f"{key_path}: identities are ed25519 keys, got {type(key).__name__}. "
            "Generate one with colca-keygen."
        )
    return key


def self_signed_cert_pem(key: Ed25519PrivateKey, common_name: str) -> bytes:
    """The PEM certificate wrapping this key's public half."""
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(hours=1))
        .not_valid_after(now + CERT_VALIDITY)
        .sign(key, None)  # ed25519 signs without a separate hash algorithm
    )
    return cert.public_bytes(serialization.Encoding.PEM)


def self_signed_context(key_path: pathlib.Path, common_name: str) -> ssl.SSLContext:
    """An SSLContext presenting a certificate minted from ``key_path``.

    The broker is trusted by pinning, so this context deliberately does not
    verify it: there is no CA, and a verification failure here would only mean
    "no authority exists", never "this broker is wrong".
    """
    key = load_identity_key(key_path)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    # ssl has no in-memory certificate API, so the certificate goes through a
    # private temporary file for the duration of the load. Only the public
    # certificate is written; the private key is read from where it already
    # lives and never leaves it.
    with tempfile.TemporaryDirectory() as tmp:
        cert_file = pathlib.Path(tmp) / "identity.crt"
        cert_file.write_bytes(self_signed_cert_pem(key, common_name))
        ctx.load_cert_chain(certfile=str(cert_file), keyfile=str(key_path))
    return ctx
