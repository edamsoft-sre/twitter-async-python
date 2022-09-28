import asyncio
import sys
import time
from json import JSONDecodeError
from typing import Any

import httpx
from fastapi import HTTPException
from httpx import Response, Request, RequestError, HTTPStatusError
from pydantic import BaseModel
from async_lru import alru_cache

from custom_logs import start_logger
from .rate_limit import RateLimit


class TwitterUser(BaseModel):
    username: str
    id: int
    name: str

    def __hash__(self):
        return hash(id)

    @staticmethod
    def from_dict(d: dict):
        return TwitterUser(id=d['id'], username=d['username'], name=d['name'])


class TwitterList(BaseModel):
    name: str
    id: int

    def __hash__(self):
        return hash(id)

    @staticmethod
    def from_dict(d: dict):
        return TwitterList(id=d['id'], name=d['name'])


class ConnectTwitter:
    """ Handle Twitter API v2 Queries with httpx async """

    def __init__(self, oauth_token: str, keepalives: int, connections: int):
        self.log = start_logger(__name__)
        self.oauth_token = oauth_token
        self.api = "https://api.twitter.com/2"

        self.keepalives = keepalives
        self.connections = connections

        self.url_followers = "/users/{}/followers"
        self.url_follows = "/users/{}/following"
        self.url_id = "/users/by?usernames={}&{}"
        self.url_lists = "/users/{}/owned_lists"
        self.url_list_members = "/lists/{}/members"
        self.rate_limit = RateLimit()

        limits = httpx.Limits(
            max_keepalive_connections=self.keepalives,
            max_connections=self.connections)
        transport = httpx.AsyncHTTPTransport(retries=2)
        self.client = httpx.AsyncClient(
            http2=True,
            limits=limits,
            transport=transport,
            headers=self.set_headers(),
            event_hooks={'request': [self.check_rate_limit],
                         'response': [self.status_errors,
                                      self.set_rate_limit]
                         }
        )

    def __del__(self):
        """ Make sure to close httpx.Client when done """
        pass

    async def close(self):
        await self.client.aclose()

    def set_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.oauth_token}",
                "User-Agent": f"Edamsoft-Twitter-Async-Python{sys.version.split(' ')[0]}-{httpx.__version__}"}

    async def check_rate_limit(self, request: Request) -> None:
        limit = self.rate_limit.get_limit(url=request.url, method=request.method)
        if limit.remaining == 0:
            s_time = max((limit.reset - time.time()), 0) + 10.0
            self.log.debug(f"Rate limited req: {request.url}")
            await asyncio.sleep(s_time)

    async def set_rate_limit(self, resp: Response):
        self.rate_limit.set_limit(url=resp.url, headers=resp.headers, method=resp.request.method)

    @staticmethod
    def process_json(response: Response) -> tuple[str | None, Any]:
        try:
            pagination = None
            results = response.json()
            if next_token := results.get('meta', {}).get('next_token'):
                pagination = next_token
        except KeyError as key_exc:
            raise HTTPException(status_code=response.status_code, detail=key_exc)
        except JSONDecodeError as json_exc:
            raise HTTPException(status_code=response.status_code, detail=json_exc)
        return pagination, results['data']

    @staticmethod
    async def status_errors(response: Response) -> None:
        try:
            response.raise_for_status()
        except HTTPStatusError as status_exc:
            if status_exc.response.status_code == 429:
                raise HTTPException(
                    status_code=429,
                    detail={
                        status_exc.response.status_code,
                        status_exc.response.headers.get('x-rate-limit-remaining')
                    }
                )
            raise HTTPException(
                status_code=status_exc.response.status_code,
                detail=response.url.path
            )

    @alru_cache(maxsize=32)
    async def get_id(self, user: str = "", fields: str = "user.fields=id") -> int:
        # "usernames=TwitterDev,TwitterAPI"
        # user_fields =  entities, id, location,
        # name, pinned_tweet_id, profile_image_url, protected,
        # public_metrics, url, username, verified, and withheld

        url = self.api + self.url_id.format(user, fields)
        try:
            resp = await self.client.get(url=url)
        except httpx.RequestError as req_exc:
            raise HTTPException(status_code=503, detail=req_exc)
        _, result = self.process_json(resp)
        return result[0]['id']

    async def get_data(self, item_id: int, url: str) -> Any:
        results = []
        try:
            resp = await self.client.get(url=url)
        except RequestError as req_exc:
            raise HTTPException(status_code=503, detail=req_exc)
        paginate, result = self.process_json(resp)
        if not paginate:
            return result
        results.extend(result)
        while paginate:
            try:
                resp = await self.client.get(url=url, params={'pagination_token': paginate})
            except RequestError as req_exc:
                raise HTTPException(status_code=503, detail=req_exc)
            paginate, result = self.process_json(resp)
            results.extend(result)
        return results

    async def get_lists_for_id(self, item_id: int, operation: str) -> list[dict]:
        url = self.api + getattr(self, f"url_{operation}").format(item_id)
        res = await self.get_data(item_id=item_id, url=url)
        return res

    async def get_followers(self, user_id: int) -> list[TwitterUser]:
        results = await self.get_lists_for_id(item_id=user_id, operation="followers")
        return list(map(TwitterUser.from_dict, results))

    async def get_follows(self, user_id: int) -> list[TwitterUser]:
        results = await self.get_lists_for_id(item_id=user_id, operation="follows")
        return list(map(TwitterUser.from_dict, results))

    async def get_lists(self, user_id: int) -> list[TwitterList]:
        results = await self.get_lists_for_id(item_id=user_id, operation="lists")
        return list(map(TwitterList.from_dict, results))

    async def get_list_members(self, list_id: int) -> list[TwitterUser]:
        results = await self.get_lists_for_id(item_id=list_id, operation="list_members")
        return list(map(TwitterUser.from_dict, results))
