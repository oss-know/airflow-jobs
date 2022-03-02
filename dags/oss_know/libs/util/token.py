import threading

from oss_know.libs.util.log import logger


class TokenManager:

    def __init__(self, tokens):
        self.idle = tokens
        self.in_use = []
        self.cool_pool = []

    def fetch(self, num):
        if num > len(self.idle):
            logger.error(f'Insufficient tokens, asking for {num}')
            return

        fetched = []
        for i in range(num):
            token = self.idle.pop()
            fetched.append(token)
            self.in_use.append(token)

        return fetched

    def fetch_all(self):
        return self.fetch(len(self.idle))

    def drop(self, token):
        """Drop an invalid token from in use list and pop a new one for replacement"""
        self.in_use.remove(token)

        if self.idle:
            return self.idle.pop()
        return None

    def cool_down(self, token, callback, recover_duration=3600):
        """Put a token which runs out of requests into the cool pool, waiting for recovery. And pop a new token

        Parameters:
            token (str): Token to cool down
            callback (func): A function to be called when token recovers
            recover_duration (int): How long should the token to wait for recovery, in seconds, default to 1 hour.
        """
        self.cool_pool.append(token)
        threading.Timer(recover_duration, self.on_token_recover, args=(token, callback)).start()

        return self.drop(token)

    def on_token_recover(self, token, callback):
        self.cool_pool.remove(token)
        # TODO It's a bit hard to decide where to put the recovered token
        # If the outside caller use the token with customized callback, then token should be an item of in_use here.
        # If the caller dones't handle the token, or pass a None callback, then token should be an item of idle.
        # Currently, the caller in GithubTokenProxyAccommodator re-use the recovered token
        self.in_use.append(token)
        if callback:
            callback(token)

    def makeup_tokens(self):
        """Get more tokens into the list if necessary"""
        # TODO It should be implementated when we have a service to provide tokens
        pass
