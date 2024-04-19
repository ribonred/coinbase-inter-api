import base64
import hashlib
import hmac
import time
import requests
from pydantic_settings import BaseSettings, SettingsConfigDict
import urllib.parse
import pandas as pd

PROD = SettingsConfigDict(
    env_prefix="COINBASE_PROD_",
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
)

DEV = SettingsConfigDict(
    env_prefix="COINBASE_DEV_",
    env_file=".env",
    env_file_encoding="utf-8",
    extra="ignore",
)


class CoinbaseSettings(BaseSettings):
    api_key: str
    api_secret: str
    passphrase: str
    base_url: str
    portfolio_id: str
    model_config: SettingsConfigDict = PROD


class CoinbaseDevSettings(CoinbaseSettings):
    model_config: SettingsConfigDict = DEV


class Settings(BaseSettings):
    mode: str
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="SETTINGS_",
    )

    def create_settings(self):
        if self.mode == "PROD":
            return CoinbaseSettings()
        elif self.mode == "DEV":
            return CoinbaseDevSettings()
        else:
            raise ValueError("Invalid settings module")


class CoinbaseIntl:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        pashphrase: str,
        account_id: str = None,
    ):
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.pashphrase = pashphrase
        self.account_id = account_id

    def _prepare_headers(
        self, signature_b64: str, timestamp: str, access_key: str, passphrase: str
    ):
        headers = {
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-ACCESS-SIGN": signature_b64,
            "CB-ACCESS-PASSPHRASE": passphrase,
            "CB-ACCESS-KEY": access_key,
        }
        return headers

    def _prepare_request(self, method, endpoint, data=None):
        url = self.base_url + endpoint
        print(url)
        timestamp = str(int(time.time()))
        message = timestamp + method + urllib.parse.urlparse(url).path + (data or "")
        key = base64.b64decode(self.api_secret)
        signature = hmac.new(
            key,
            message.encode("utf-8"),
            hashlib.sha256,
        )
        signature_b64 = base64.b64encode(signature.digest()).decode("utf-8")
        headers = self._prepare_headers(
            signature_b64, timestamp, self.api_key, self.pashphrase
        )
        response = requests.request(method, url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()

    def get_portofolios(self) -> dict:
        return self._prepare_request("GET", "/api/v1/portfolios")

    def get_order_fills(self) -> dict:
        return self._prepare_request(
            "GET", f"/api/v1/portfolios/{self.account_id}/fills"
        )

    def get_transfers(self) -> dict:
        return self._prepare_request(
            "GET",
            f"/api/v1/transfers?portfolios={self.account_id}&result_limit=100&type=ALL",
        )

    def get_balances(self) -> dict:
        return self._prepare_request(
            "GET", f"/api/v1/portfolios/{self.account_id}/balances"
        )

    def get_positions(self) -> dict:
        return self._prepare_request(
            "GET", f"/api/v1/portfolios/{self.account_id}/positions"
        )

    def get_summary(self) -> dict:
        return self._prepare_request(
            "GET", f"/api/v1/portfolios/{self.account_id}/summary"
        )


settings = Settings()
creds = settings.create_settings()
client = CoinbaseIntl(
    creds.base_url,
    creds.api_key,
    creds.api_secret,
    creds.passphrase,
    creds.portfolio_id,
)
fills = pd.DataFrame().from_records(client.get_order_fills()["results"])
transfers = pd.DataFrame().from_records(client.get_transfers()["results"])
balances = pd.DataFrame().from_records(client.get_balances())
positions = pd.DataFrame().from_records(client.get_positions())
portfolio_summary = pd.DataFrame().from_records([client.get_summary()])
fills.to_csv("order_fills.csv", index=False)
transfers.to_csv("transfers.csv", index=False)
balances.to_csv("balances.csv", index=False)
positions.to_csv("positions.csv", index=False)
portfolio_summary.to_csv("portfolio_summary.csv", index=False)
