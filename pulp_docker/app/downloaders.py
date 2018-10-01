from gettext import gettext as _
from logging import getLogger
from urllib import parse
import asyncio
import backoff
import json
import re

from aiohttp.client_exceptions import ClientResponseError

from pulpcore.plugin.download import HttpDownloader


logger = getLogger(__name__)


class TokenAuthHttpDownloader(HttpDownloader):
    """
    TODO(asmacdo)
    """

    def __init__(self, *args, **kwargs):
        """
        TODO(asmacdo)
        """
        self._auth_header = kwargs.pop('token', None)
        super().__init__(*args, **kwargs)
        self.token_lock = asyncio.Lock()
        self.header_is_current = asyncio.Event()
        self.header_is_current.set()

    # TODO(asmacdo) backoff
    # @backoff.on_exception(backoff.expo, aiohttp.ClientResponseError, max_tries=10, giveup=giveup)
    async def run(self, handle_401=True):
        """
        Download, validate, and compute digests on the `url`. This is a coroutine.

        This method is decorated with a backoff-and-retry behavior to retry HTTP 429 errors. It
        retries with exponential backoff 10 times before allowing a final exception to be raised.

        This method provides the same return object type and documented in
        :meth:`~pulpcore.plugin.download.BaseDownloader.run`.

        TODO handle_401(bool): If true, catch 401, request a new token and retry.
        """
        await self.header_is_current.wait()
        async with self.session.get(self.url, headers=self.auth_header) as response:
            try:
                response.raise_for_status()
            except ClientResponseError as e:
                response_auth_header = response.headers.get('www-authenticate')
                if handle_401 and e.status == 401 and response_auth_header is not None:
                    if not self.token_lock.locked():
                        await self.update_token(response_auth_header)
                    return await self.run(handle_401=False)
                else:
                    raise
            to_return = await self._handle_response(response)
            await response.release()

        if self._close_session_on_finalize:
            self.session.close()
        return to_return

    async def update_token(self, response_auth_header):
        """
        TODO lock
        """
        async with self.token_lock:
            self.header_is_current.clear()
            bearer_info_string = response_auth_header[len("Bearer "):]
            bearer_info_list = re.split(',(?=[^=,]+=)', bearer_info_string)

            # The remaining string consists of comma seperated key=value pairs
            auth_query_dict = {}
            for key, value in (item.split('=') for item in bearer_info_list):
                # The value is a string within a string, ex: '"value"'
                auth_query_dict[key] = json.loads(value)
            try:
                token_base_url = auth_query_dict.pop('realm')
            except KeyError:
                raise IOError(_("No realm specified for token auth challenge."))

            # Construct a url with query parameters containing token auth challenge info
            parsed_url = parse.urlparse(token_base_url)
            # Add auth query params to query dict and urlencode into a string
            new_query = parse.urlencode({**parse.parse_qs(parsed_url.query), **auth_query_dict})
            updated_parsed = parsed_url._replace(query=new_query)
            token_url = parse.urlunparse(updated_parsed)

            async with self.session.get(token_url, raise_for_status=True) as token_response:
                token_data = await token_response.text()

            new_token = json.loads(token_data)['token']
            self.auth_header = new_token
            self.header_is_current.set()

    @property
    def auth_header(self):
        return self._auth_header

    @auth_header.setter
    def auth_header(self, token):
        self._auth_header = {'Authorization': 'Bearer {token}'.format(token=token)}

    def parse_401_response_headers(self, auth_header):
        """
        Parse the www-authenticate header from a 401 response into a dictionary that contains
        the information necessary to retrieve a token.

        :param auth_header: www-authenticate header returned in a 401 response
        :type  auth_header: basestring
        """
        auth_header = auth_header[len("Bearer "):]
        auth_header = re.split(',(?=[^=,]+=)', auth_header)

        # The remaining string consists of comma seperated key=value pairs
        auth_dict = {}
        for key, value in (item.split('=') for item in auth_header):
            # The value is a string within a string, ex: '"value"'
            auth_dict[key] = json.loads(value)
        return auth_dict
