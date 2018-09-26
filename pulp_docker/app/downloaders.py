from gettext import gettext as _
from logging import getLogger
from urllib import parse
import backoff
import json
import re

# TODO(amsacdo) consolidate aiohttp imports
from aiohttp.client_exceptions import ClientResponseError

from pulpcore.plugin.download import HttpDownloader


logger = getLogger(__name__)


class TokenAuthHttpDownloader(HttpDownloader):
    """
    TODO(asmacdo) catch 401
    """
    # TODO(asmacdo) backoff?
    # @backoff.on_exception(backoff.expo, aiohttp.ClientResponseError, max_tries=10, giveup=giveup)
    async def run(self, retry=True):
        """
        Download, validate, and compute digests on the `url`. This is a coroutine.

        This method is decorated with a backoff-and-retry behavior to retry HTTP 429 errors. It
        retries with exponential backoff 10 times before allowing a final exception to be raised.

        This method provides the same return object type and documented in
        :meth:`~pulpcore.plugin.download.BaseDownloader.run`.
        """
        async with self.session.get(self.url) as response:

            try:
                response.raise_for_status()
            except ClientResponseError as e:
                auth_header = response.headers.get('www-authenticate')
                if retry and e.status == 401 and auth_header is not None:
                    await self.update_headers(auth_header)
                    return await self.run(retry=False)
                else:
                    raise
            to_return = await self._handle_response(response)
            await response.release()

        if self._close_session_on_finalize:
            self.session.close()
        return to_return

    async def update_headers(self, auth_header):
        auth_info = self.parse_401_token_response_headers(auth_header)
        try:
            token_url = auth_info.pop('realm')
        except KeyError:
            # TODO(asmacdo) is this correct?
            raise IOError(_("No realm specified for token auth challenge."))

        # Construct a url with query parameters containing token auth challenge info
        parse_result = parse.urlparse(token_url)
        query_dict = parse.parse_qs(parse_result.query)
        query_dict.update(auth_info)
        url_pieces = list(parse_result)
        url_pieces[4] = parse.urlencode(query_dict)
        token_url = parse.urlunparse(url_pieces)

        new_token = await self.fetch_token(token_url)
        # TODO(asmacdo) use of private method `_default_headers` is discouraged
        # `_prepare_headers` is not specifically included in discouraged ATTRS.
        # https://github.com/aio-libs/aiohttp/blob/master/aiohttp/client.py#L76
        # Issue: https://github.com/aio-libs/aiohttp/issues/3299
        self.session._default_headers = self.session._prepare_headers(
            {'Authorization': 'Bearer %s' % new_token}
        )

    async def fetch_token(self, url):

        async with self.session.get(url, raise_for_status=True) as token_response:
            token_data = await token_response.text()
            import time
            time.sleep(1)
        return json.loads(token_data)['token']

    def parse_401_token_response_headers(self, auth_header):
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
