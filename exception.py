

class AuthenticationException(Exception):

    def __init__(self, message="Invalid credential"):
        self.message = message
        super().__init__(self.message)


class ServerErrorException(Exception):

    def __init__(self, message="Something went wrong"):
        self.message = message
        super().__init__(self.message)
