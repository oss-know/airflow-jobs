import itertools
from random import shuffle as shuffle_list

import requests
from requests.exceptions import RequestException

from oss_know.libs.util.log import logger
from oss_know.libs.util.token import TokenManager


class ProxyService:

    def __init__(self, api_url):
        self.api_url = api_url

    def fetch(self, num):
        pass


class KuaiProxyService(ProxyService):
    def __init__(self, api_url, orderid, include_cities=[]):
        super(KuaiProxyService, self).__init__(api_url)
        self.orderid = orderid
        self.include_cities = include_cities

    def fetch(self, num, include_cities=[]):
        # TODO Handle more types of schemes and urls(some might need username and password)
        params = {
            'pt': 1,  # Type of proxy, 1 for http, 2 for socks
            'sep': 1,  # Separator, 1 for \r\n, 2 for \n
            'orderid': self.orderid
        }
        headers = {
            "Accept-Encoding": "gzip"  # Accept gzip for faster access
        }

        # Sample url: 'http://dps.kdlapi.com/api/getdps/?orderid=964584821406045&num=6&pt=1&sep=1'
        res = requests.get(self.api_url, params=params, headers=headers)
        res.raise_for_status()

        proxies = []
        for line in str.split(res.text):
            proxies.append(f'http://{line}')
        return proxies


class _token_proxy_iter:
    """An iterator to provide (token, proxy) pair"""

    def __init__(self, tokens):
        self.tokens = tokens
        self.tokens_iter = itertools.cycle(tokens)

    def next(self):
        """Get next (token, proxy) pair"""
        pass


class _cycle_iterToken(_token_proxy_iter):
    """Both tokens and proxies are iterated in cycles"""

    def __init__(self, tokens, proxies):
        super().__init__(tokens)
        self.proxies_iter = itertools.cycle(proxies)

    def next(self):
        return next(self.tokens_iter), next(self.proxies_iter)


class _fixed_map_iterToken(_token_proxy_iter):
    """Each token is mapped to a specific proxy"""

    def __init__(self, tokens, proxies):
        super().__init__(tokens)

        self.mapping = {}
        num_proxies = len(proxies)
        for index, token in enumerate(tokens):
            mapped_index = index % num_proxies
            self.mapping[token] = proxies[mapped_index]

    def next(self):
        # TODO Is random.choice a better choice(less mem consuming)?
        # token = random.choice(self.tokens)
        token = next(self.tokens_iter)
        proxy = self.mapping[token]
        return token, proxy

    def update_mapping(self, token, new_proxy):
        # if token not in self.mapping:
        #     logger.log(f'{token} not in iter mapping, skip')
        #     return
        self.mapping[token] = new_proxy

    def replace_mapping(self, token, new_token):
        if token in self.tokens:
            self.tokens.remove(token)
            self.renew_tokens(self.tokens)
        paired_proxy = self.mapping.pop(token, None)
        # paired_proxy should never be None
        if new_token and paired_proxy:
            # TODO Shall we assign the paired proxy to the new token?
            # Share: The proxy takes 2 tokens, this saves proxies, but might be considered hack by github server?
            # Don't share: Costs more proxies, but make the program 'looks' safer :)
            self.mapping[new_token] = paired_proxy

    def renew_tokens(self, tokens):
        # self.tokens = tokens # TODO Is this needed?
        self.tokens_iter = itertools.cycle(tokens)

    def get_proxy(self, token):
        if token in self.mapping:
            return self.mapping[token]
        return None


class ProxyManager:
    def __init__(self, proxies, proxy_service: ProxyService):
        self.pool = proxies  # TODO Not used yet
        self.idle = proxies
        self.proxy_service = proxy_service
        self.in_use = []
        self.index = 0

    def _makeup_proxies(self, num):
        self.idle += self.proxy_service.fetch(num)

    def fetch(self, num):
        if num > len(self.idle):
            self._makeup_proxies(num)

        fetched = []
        for i in range(num):
            p = self.idle.pop()
            fetched.append(p)
            self.in_use.append(p)
        return fetched

    def fetch_all(self):
        return self.fetch(len(self.idle))

    def idle_proxy(self, proxy):
        # A proxy might be bound to multiple tokens, when tokens become invalid and try to idle the paired proxy,
        # we should check proxy existence first, making sure the proxy is neither removed multi times in in_use list,
        # nor appended multi times in idle list.
        if proxy not in self.idle:
            self.idle.append(proxy)

        if proxy not in self.in_use:
            logger.warning(f'Proxy {proxy} not found in "in use" list')
            return
        self.in_use.remove(proxy)

    def report_invalid(self, proxy):
        if proxy in self.in_use:
            self.in_use.remove(proxy)
        else:
            logger.warning(f'Proxy {proxy} not found in "in use" list')

        try:
            if not self.idle:
                self._makeup_proxies(10)
            return self.idle.pop()
        except RequestException as e:
            logger.error(f'Failed to make up proxies: {e}')
            return None


class GithubTokenProxyAccommodator:
    """An helper class to accommodate token and proxy"""
    POLICY_CYCLE_ITERATION = 'cycle_iteration'
    POLICY_FIXED_MAP = 'fixed_map'

    def __init__(self, token_manager: TokenManager, proxy_manager: ProxyManager, policy='cycle_iteration',
                 shuffle=True):
        """
        :param tokens: List of github tokens
        :param proxies: List of proxies dict with form {"scheme":"", "ip": "", "port":123, "user": "", "password": ""}
        :param policy: Accommodation policy, currently support POLICY_CYCLE_ITERATION and POLICY_FIXED_MAP. In
        POLICY_CYCLE_ITERATION'
        policy, both tokens and proxies are iterated in a cycle. In POLICY_FIXED_MAP policy, a token pairs with a
        fixed proxy,
        all tokens are paired.
        :param shuffle: Whether tokens and proxies should be shuffled before building accommodator.
        This allows some randomity
        """
        self.tokens = token_manager.fetch_all()
        num_tokens = len(self.tokens)
        num_proxies = len(proxy_manager.idle)
        self.proxies = proxy_manager.fetch_all() if num_tokens >= num_proxies else proxy_manager.fetch(num_tokens)

        if shuffle:
            shuffle_list(self.tokens)
            shuffle_list(self.proxies)
        self.token_manager = token_manager
        self.proxy_manager = proxy_manager

        self.policy = policy
        self._accommodate()

    def _accommodate(self):
        if self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            self._iter = _cycle_iterToken(self.tokens, self.proxies)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter = _fixed_map_iterToken(self.tokens, self.proxies)

    def _update(self, token, proxy):
        if self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            self._iter = _cycle_iterToken(self.tokens, self.proxies)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter.mapping[token] = proxy

    def next(self):
        return self._iter.next()

    def report_invalid_proxy(self, token, proxy):
        if proxy in self.proxies:
            self.proxies.remove(proxy)
        else:
            logger.warning(f'Proxy {proxy} not in proxies list')

        new_proxy = self.proxy_manager.report_invalid(proxy)
        if new_proxy:
            self.proxies.append(new_proxy)
        else:
            logger.warning(f'Failed to get new proxy, proxy list might run out')

        if self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter.update_mapping(token, new_proxy)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            # For simple cycle iterations, just re-init all tokens and proxies
            self._accommodate()

    def report_invalid_token(self, token):
        """The paired proxy of the invalid token is still available, assign it to the new token if possible"""
        logger.info(f'Report invalid token {token}')
        if token in self.tokens:
            self.tokens.remove(token)
        else:
            logger.warning(f'Token {token} not in tokens list')

        new_token = self.token_manager.drop(token)
        if new_token:
            self.tokens.append(new_token)

        if self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter.renew_tokens(self.tokens)  # token iter's token list is updated

            # We should fetch and release the proxy before replacing mapping
            released_proxy = self._iter.get_proxy(token)
            self.proxy_manager.idle_proxy(released_proxy)

            # If new_token is None, replace_mapping just delete the old one
            self._iter.replace_mapping(token, new_token)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            # For simple cycle iterations, just re-init all tokens and proxies
            self._accommodate()

    def report_drain_token(self, token, recover_duration=3600):
        logger.info(f'Report drain token {token}')

        if token in self.tokens:
            self.tokens.remove(token)
        else:
            logger.warning(f'Token {token} not in tokens list')
        new_token = self.token_manager.cool_down(token, self.on_token_wakeup, recover_duration=recover_duration)
        if new_token:
            self.tokens.append(new_token)

        if self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter.renew_tokens(self.tokens)  # token iter's token list is updated
            # If new_token is None, replace_mapping just delete the old one
            self._iter.replace_mapping(token, new_token)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            # For simple cycle iterations, just re-init all tokens and proxies
            self._accommodate()

    def on_token_wakeup(self, token):
        self.tokens.append(token)
        if self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter.renew_tokens(self.tokens)  # token iter's token list is updated
            new_proxy = self.proxy_manager.fetch(1)[0]
            self._iter.update_mapping(token, new_proxy)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            # For simple cycle iterations, just re-init all tokens and proxies
            self._accommodate()
