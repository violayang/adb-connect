import os
import oracledb  # rename of cx_oracle
import pandas as pd
import numpy as np
import oci
from base64 import b64decode

# set TNS_ADMIN
os.environ['TNS_ADMIN'] = "/home/datascience/...<wallet_file_path>"


# Function to get secrets from vault
def get_sec(secret_id):
    ociConfigFilePath = "~/.oci/config"
    ociProfileName = "DEFAULT"
    config = oci.config.from_file(ociConfigFilePath, ociProfileName)

    try:
        secrets_client = oci.secrets.SecretsClient(config)
        get_secret_response = secrets_client.get_secret_bundle(secret_id)
        value = b64decode(get_secret_response.data.secret_bundle_content.content.encode()).decode()
        # print("secret: ", str(value))

    except Exception as e:
        print(e)

    return value


# Fetch ADB credential
sec_id = 'ocid1.vaultsecret.oc1....'  # Secret OCID
passwd = get_sec(sec_id)

# enable thick mode
oracledb.init_oracle_client()

# connect
try:
    connection = oracledb.connect(
        user="<db_user>",
        password=passwd,  # database password
        dsn="xxx_high",  # TNS Alias from tnsnames.ora
        # config_dir=os.environ['TNS_ADMIN'],        # directory with tnsnames.ora
        # wallet_location=os.environ['TNS_ADMIN']  # directory with ewallet.pem
        # wallet_password=passwd
    )

except Exception as e:
    print(e)

# Read table
cursor = connection.cursor()
result = cursor.execute("SELECT * FROM SH.CUSTOMERS;")

data = result.fetchall()
df = pd.DataFrame(np.array(data))
print(df.shape())
