from cryptography.fernet import Fernet
import os

KEY = os.getenv("ENCRYPTION_KEY")
if not KEY:
    KEY = Fernet.generate_key()

try:
    cipher = Fernet(KEY)
except Exception as e:
    raise RuntimeError(
        "Invalid ENCRYPTION_KEY. It must be a 32-byte url-safe base64 string. "
        "Generate one with: python3 -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
    ) from e

def encrypt_api_key(api_key: str) -> str:
    return cipher.encrypt(api_key.encode()).decode()

def decrypt_api_key(encrypted_key: str) -> str:
    return cipher.decrypt(encrypted_key.encode()).decode()
