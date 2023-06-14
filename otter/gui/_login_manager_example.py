class LoginManager:

    def authenticate(self, user: str, passwd: str) -> bool:
        return (user, passwd) == ("adam", "pass")
