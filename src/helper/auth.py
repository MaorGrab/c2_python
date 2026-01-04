from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
import base64
from asyncio import StreamWriter

import helper.constants as constants


def get_spki_hash(der_cert_bytes: bytes) -> str:
    cert = x509.load_der_x509_certificate(der_cert_bytes)
    public_key = cert.public_key()
    spki_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    digest = hashes.Hash(hashes.SHA256())
    digest.update(spki_bytes)
    return base64.b64encode(digest.finalize()).decode("ascii")

def validate_server_certificate(writer: StreamWriter) -> bool:
    ssl_obj = writer.get_extra_info("ssl_object")
    der_cert = ssl_obj.getpeercert(binary_form=True)
    server_sha_received = get_spki_hash(der_cert)
    if server_sha_received == constants.SERVER_SHA:
        return True
    return False