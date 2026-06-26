"""
gen_keys.py  —  creates a key pair so dbt can log in to Snowflake without MFA.
Run it once:  python gen_keys.py
It writes the private key to  C:\\Users\\<you>\\.dbt\\rsa_key.p8
and prints the public key for you to paste into Snowflake.
"""
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

dbt_dir = os.path.join(os.path.expanduser("~"), ".dbt")
os.makedirs(dbt_dir, exist_ok=True)
priv_path = os.path.join(dbt_dir, "rsa_key.p8")

# 1) generate a 2048-bit private key
key = rsa.generate_private_key(
    public_exponent=65537, key_size=2048, backend=default_backend()
)

# 2) save the PRIVATE key (unencrypted, so dbt needs no passphrase)
with open(priv_path, "wb") as f:
    f.write(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))

# 3) build the PUBLIC key as a single line (what Snowflake wants)
pub_pem = key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()
pub_body = "".join(l for l in pub_pem.splitlines() if "KEY-----" not in l)

print("\nPrivate key saved to:", priv_path)
print("\n================ COPY THE LINE BELOW ================\n")
print(pub_body)
print("\n====================================================\n")
print("Paste it into Snowflake like this (as ACCOUNTADMIN):")
print("  ALTER USER ADMIN SET RSA_PUBLIC_KEY='<paste the line>';\n")