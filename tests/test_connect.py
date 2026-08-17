"""What `autocreate_and_connect` configures — identity, pinning, session."""
import ssl
from unittest.mock import patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from paho.mqtt.client import Client as PahoClient, MQTTv5

from franzmq.client import Client


@pytest.fixture
def key_file(tmp_path, monkeypatch):
    key = Ed25519PrivateKey.generate()
    path = tmp_path / "m1.key"
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    monkeypatch.setenv("MACHINE_KEY", str(path))
    monkeypatch.delenv("NODE_ID", raising=False)
    return path


@pytest.fixture
def connected(key_file):
    with patch.object(PahoClient, "connect") as connect:
        client = Client.autocreate_and_connect("m1")
    return client, connect


def test_the_client_speaks_mqtt_5():
    # On 3.1.1 a PUBACK has no reason field, so a rejected publish reads as
    # success. Every client this library builds must be on 5.
    assert Client()._protocol == MQTTv5


def test_identity_is_the_client_id_the_username_and_the_topic_level(connected):
    client, _ = connected
    assert client.node_id == "m1"
    assert client._client_id == b"m1"
    assert client._username == b"m1"
    assert client._password is None


def test_node_id_from_the_environment_wins(key_file, monkeypatch):
    monkeypatch.setenv("NODE_ID", "edge-1")
    with patch.object(PahoClient, "connect"):
        client = Client.autocreate_and_connect("some-client")
    assert client.node_id == "edge-1"
    assert client._username == b"edge-1"


def test_the_broker_is_trusted_by_pinning_not_by_a_ca(connected):
    client, _ = connected
    assert client._ssl_context.verify_mode == ssl.CERT_NONE
    assert client._ssl_context.get_ca_certs() == []


def test_the_session_survives_a_disconnect(connected):
    _, connect = connected
    kwargs = connect.call_args.kwargs
    assert kwargs["clean_start"] is False
    # Commands issued while this client was away must still be waiting for it.
    assert kwargs["properties"].SessionExpiryInterval > 0


def test_a_missing_key_says_what_to_do(monkeypatch):
    monkeypatch.delenv("MACHINE_KEY", raising=False)
    with pytest.raises(RuntimeError, match="MACHINE_KEY"):
        Client.autocreate_and_connect("m1")


def test_a_key_path_that_does_not_exist_is_named(monkeypatch, tmp_path):
    monkeypatch.setenv("MACHINE_KEY", str(tmp_path / "absent.key"))
    with pytest.raises(RuntimeError, match="does not exist"):
        Client.autocreate_and_connect("m1")
