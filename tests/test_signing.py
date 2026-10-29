import hashlib
import hmac

from relayqueue.signing import sign


def test_signature_matches_protocol_and_binds_id_and_timestamp():
    body = b'{"topic":"order.created","data":{}}'
    expected = (
        "v1="
        + hmac.new(
            b"secret", b'1700000000.evt-1.{"topic":"order.created","data":{}}', hashlib.sha256
        ).hexdigest()
    )
    assert sign("secret", "evt-1", "1700000000", body) == expected
    assert sign("secret", "evt-2", "1700000000", body) != expected
    assert sign("secret", "evt-1", "1700000001", body) != expected
