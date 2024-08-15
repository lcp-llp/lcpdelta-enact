class EnactApiError(Exception):
    def __init__(self, error_code, message, response):
        self.error_code = error_code
        self.message = message
        self.response = response
        super().__init__(f"{error_code}: {message}")
