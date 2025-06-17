from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
import aiohttp, aiofiles, asyncio, os, json, logging, datetime, nest_asyncio, orjson, time

nest_asyncio.apply()
logging.basicConfig(
    level=logging.INFO, format="%(message)s", handlers=[logging.StreamHandler()]
)

DATA_ROOT = "/dbfs/FileStore/DataProduct/DataArchitecture/Navigator"


@dataclass
class APIConfig:
    SERVICE_HOST: str = "app-dynamic-api-cus-staging.azurewebsites.net"
    TOKEN_URL: str = "https://techf-dynamic-stg.us.auth0.com/oauth/token"
    AUDIENCE: str = "https://dynamic-api-stg.techf.com"
    CLIENT_ID: str = "L5MEv8hiXZgNHGnZbXa2tigtHYurqIix"
    CLIENT_SECRET: str = (
        "AVceqskM__gANs_L6v6tDCQLyqyX0lWkTvUvz9FPRP0IN8K7cpu_b4svBQ1yhXY6"
    )


class SyncManager:
    TIMESTAMP_FILE = f"{DATA_ROOT}/last_sync_time.txt"
    TMP_FILE = f"{DATA_ROOT}/last_sync_time.tmp"

    @classmethod
    async def load_last_sync_time(cls) -> Optional[str]:
        try:
            if os.path.exists(cls.TIMESTAMP_FILE):
                async with aiofiles.open(cls.TIMESTAMP_FILE, "r") as f:
                    t = (await f.read()).strip()
                    return t or None
        except Exception as e:
            logging.error({"event": "error_reading_last_sync_time", "error": str(e)})
        return None

    @classmethod
    async def save_sync_time(cls, sync_time: str):
        try:
            async with aiofiles.open(cls.TMP_FILE, "w") as f:
                await f.write(sync_time)
            os.replace(cls.TMP_FILE, cls.TIMESTAMP_FILE)
            logging.info({"event": "sync_time_saved", "time": sync_time})
        except Exception as e:
            logging.error({"event": "error_saving_sync_time", "error": str(e)})

    @staticmethod
    def now_utc_iso() -> str:
        return datetime.datetime.now(datetime.timezone.utc).isoformat()

    @staticmethod
    def sync_id() -> str:
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


class NavigatorAPI:
    ENDPOINTS = {
        "user": {"page_size": 100},
        "client": {"page_size": 20},
        "case": {
            "page_size": 20,
            "special_content_type": True,
            "treat_500_as_success": True,
        },
    }

    def __init__(self, config: APIConfig, sync_id: str):
        self.config = config
        self.sync_id = sync_id
        self.token: Optional[str] = None
        self.token_expires_at: Optional[datetime.datetime] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_hits = 0
        self.cb_errors = 0
        self.cb_max_errors = 5
        self.cb_open = False
        self.cb_timeout = 60
        self.cb_reset_time = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def get_token(self):
        if not self.token or self._token_expired():
            await self._authenticate()
        return self.token

    def _token_expired(self) -> bool:
        if not self.token_expires_at:
            return True
        return (
            self.token_expires_at - datetime.datetime.now(datetime.timezone.utc)
        ).total_seconds() < 30

    async def _authenticate(self):
        payload = {
            "client_id": self.config.CLIENT_ID,
            "client_secret": self.config.CLIENT_SECRET,
            "audience": self.config.AUDIENCE,
            "grant_type": "client_credentials",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.config.TOKEN_URL, json=payload) as r:
                r.raise_for_status()
                d = await r.json()
                self.token = d["access_token"]
                self.token_expires_at = datetime.datetime.now(
                    datetime.timezone.utc
                ) + datetime.timedelta(seconds=int(d.get("expires_in", 86400)))

    async def request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        special_content_type: bool = False,
    ):
        retries, delay = 0, 1
        while retries < 5:
            if self.cb_open:
                if time.time() > self.cb_reset_time:
                    self.cb_open = False
                    self.cb_errors = 0
                    logging.info({"event": "circuit_breaker_closed"})
                else:
                    await asyncio.sleep(1)
                    continue
            try:
                token = await self.get_token()
                url = urljoin(f"https://{self.config.SERVICE_HOST}", endpoint)
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
                async with self.session.request(
                    method, url, headers=headers, json=payload, timeout=60
                ) as r:
                    if r.status == 401:
                        await self._authenticate()
                        continue
                    if r.status == 429:
                        self.rate_limit_hits += 1
                        retry_after = int(r.headers.get("Retry-After", "1"))
                        await asyncio.sleep(retry_after)
                        continue
                    if r.status >= 500:
                        self.cb_errors += 1
                        if self.cb_errors >= self.cb_max_errors:
                            self.cb_open = True
                            self.cb_reset_time = time.time() + self.cb_timeout
                            logging.error({"event": "circuit_breaker_opened"})
                        raise Exception(f"Server error: {r.status}")
                    r.raise_for_status()
                    content_type = r.headers.get("Content-Type", "")
                    if special_content_type and "application/json" not in content_type:
                        text = await r.text()
                        return json.loads(text)
                    return await r.json()
            except Exception as e:
                retries += 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
                if retries >= 5:
                    raise

    async def fetch_all(
        self, name: str, updated_on_after_date: Optional[str]
    ) -> List[Dict[str, Any]]:
        config = self.ENDPOINTS[name]
        page_size = config["page_size"]
        special_content_type = config.get("special_content_type", False)
        treat_500_as_success = config.get("treat_500_as_success", False)
        results, token, page = [], None, 0
        warn_size = False
        try:
            while True:
                payload = {
                    "continuationToken": token,
                    "pageSize": page_size,
                    "updatedOnAfterDate": updated_on_after_date,
                }
                try:
                    resp = await self.request(
                        "POST",
                        f"/customerapi/{name}/search",
                        payload,
                        special_content_type=special_content_type,
                    )
                    items = resp.get("items", [])
                    resp_size = resp.get("pageSize", page_size)
                    if resp_size < page_size and not warn_size:
                        logging.warning(
                            {
                                "event": "api_capped_page_size",
                                "requested": page_size,
                                "returned": resp_size,
                                "endpoint": name,
                            }
                        )
                        warn_size = True
                    await self.save_page(resp, name, page)
                    results.extend(items)
                    token = resp.get("continuationToken")
                    if (not items and not token) or token is None:
                        break
                    page += 1
                except Exception as e:
                    # For "case", treat any 500 as a success and finish

                    if treat_500_as_success and "Server error" in str(e):
                        logging.warning(
                            {
                                "event": f"{name}_treating_final_server_error_as_success",
                                "info": str(e),
                                "count": len(results),
                            }
                        )
                        break
                    # For other endpoints, re-raise the exception

                    raise
        except Exception as e:
            # Only reached for user/client or unexpected errors

            if results:
                logging.warning(
                    {
                        "event": f"{name}_partial_success_with_error",
                        "error": str(e),
                        "count": len(results),
                    }
                )
                raise PartialSuccessException(results, str(e))
            else:
                raise
        return results

    async def save_page(self, data: Dict[str, Any], endpoint: str, page: int):
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        dir_path = f"{DATA_ROOT}/Search/JSON/{endpoint}/{self.sync_id}"
        os.makedirs(dir_path, exist_ok=True)
        file_name = f"{dir_path}/{timestamp}_page_{page}.json"
        async with aiofiles.open(file_name, "w", encoding="utf-8") as f:
            await f.write(
                orjson.dumps(data, option=orjson.OPT_INDENT_2).decode("utf-8")
            )
        logging.info(
            {"event": "page_saved", "endpoint": endpoint, "file_path": file_name}
        )


class PartialSuccessException(Exception):
    def __init__(self, results, message):
        self.results = results
        self.message = message
        super().__init__(message)


async def main():
    os.makedirs(DATA_ROOT, exist_ok=True)
    sync_id = SyncManager.sync_id()
    last_sync = await SyncManager.load_last_sync_time()
    updated_after = last_sync
    logging.info({"event": "main_started"})
    logging.info({"event": "sync_id_set", "sync_id": sync_id})
    if last_sync:
        logging.info({"event": "last_sync_time_loaded", "time": last_sync})
    else:
        logging.info({"event": "no_previous_sync_found", "performing": "full_sync"})
    results, errors, record_counts, rate_limit_hits, partial_success = (
        {},
        {},
        {},
        {},
        {},
    )
    async with NavigatorAPI(APIConfig(), sync_id) as api:
        for ep in NavigatorAPI.ENDPOINTS:
            logging.info({"event": f"fetching_{ep}s", "updated_after": updated_after})
            try:
                recs = await api.fetch_all(ep, updated_after)
                results[ep], record_counts[ep], rate_limit_hits[ep] = (
                    recs,
                    len(recs),
                    api.rate_limit_hits,
                )
                logging.info({"event": f"{ep}_fetch_success", "count": len(recs)})
                partial_success[ep] = False
            except PartialSuccessException as p:
                results[ep], record_counts[ep], errors[ep], partial_success[ep] = (
                    p.results,
                    len(p.results),
                    f"Partial success: {p.message}",
                    True,
                )
                logging.warning(
                    {
                        "event": f"{ep}_partial_success",
                        "count": len(p.results),
                        "error": p.message,
                    }
                )
            except Exception as e:
                errors[ep], record_counts[ep], partial_success[ep] = str(e), 0, False
                logging.error({"event": f"{ep}_fetch_failed", "error": str(e)})
    # Generate a new sync timestamp after processing all endpoints
    sync_time = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=1)
    ).isoformat()

    summary = {
        "event": "sync_summary",
        "timestamp": sync_time,
        "sync_id": sync_id,
        "endpoint_results": {
            ep: {
                "records_count": record_counts.get(ep, 0),
                "error": errors.get(ep),
                "rate_limit_hits": rate_limit_hits.get(ep, 0),
                "partial_success": partial_success.get(ep, False),
            }
            for ep in NavigatorAPI.ENDPOINTS
        },
    }
    logging.info(summary)
    # Advance timestamp if all endpoints succeeded or case finished with server error

    should_advance = True
    for ep in NavigatorAPI.ENDPOINTS:
        if ep == "case":
            continue
        if errors.get(ep) and not partial_success.get(ep):
            should_advance = False
            break
    if should_advance:
        await SyncManager.save_sync_time(sync_time)
    else:
        logging.warning(
            {"event": "sync_timestamp_not_advanced_due_to_errors", "errors": errors}
        )
    logging.info({"event": "main_completed"})


if __name__ == "__main__":
    asyncio.run(main())
