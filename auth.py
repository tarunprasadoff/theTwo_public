class Auth:

    def __init__(self):

        self.key = '' # Add your API Key
        self.secret = '' # Add your API Secret
        self.secret_bytes = bytes(self.secret, encoding='utf-8')