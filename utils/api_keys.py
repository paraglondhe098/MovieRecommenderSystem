import os
import pickle
import getpass
import base64
from functools import wraps
from typing import final


class FinalMeta(type):
    def __new__(mcs, name, bases, class_dict):
        if any(isinstance(base, FinalMeta) for base in bases):
            raise TypeError(f"Subclassing of {bases[0].__name__} is not allowed!")
        return super().__new__(mcs, name, bases, class_dict)


class API_Keys(metaclass=FinalMeta):
    def __init__(self, requires_pass=True, filename=None):
        self.__secret_attr_00764354 = None
        self.__keys = {}
        self.__requires_pass = requires_pass
        self.__filename = filename or os.path.join(os.getcwd(), "utils\\API_KEYS.pkl")
        self.__password = None  # Password will be loaded or set to default
        self.load_from_file()
        del self.__secret_attr_00764354

        if not self.__password:
            self.__password = self.encode("123")  # Default password

    @staticmethod
    def encode(data):
        return base64.b64encode(data.encode()).decode()

    @staticmethod
    def decode(encoded_data):
        return base64.b64decode(encoded_data.encode()).decode()

    def load_from_file(self):
        try:
            with open(self.__filename, 'rb') as file:
                data = pickle.load(file)
                self.__keys = data.get('keys', {})
                self.__password = data.get('password', None)
                print("Key loading successful.")
        except (FileNotFoundError, EOFError):
            print(f"No existing data found. Starting with an empty keychain.")

        if self.__password is None:
            self.__password = self.encode("123")

    def save_to_file(self):
        data = {
            'keys': self.__keys,
            'password': self.__password
        }
        with open(self.__filename, 'wb') as file:
            pickle.dump(data, file)
        print("Keys and password successfully saved to file.")

    def set_key(self, platform, key):
        if platform in list(self.__keys.keys()):
            if self.__requires_pass:
                if not self.__check_password("Password is required to modify existing key: "):
                    return
            else:
                inp = input(f"Change existing key for {platform}? (Y/N) ")
                if inp != "Y":
                    print("Key is not modified!")
                    return
            print("Modified existing key")
        else:
            print(f"Added new key for {platform}")
        self.__keys[platform] = self.encode(key)
        self.save_to_file()

    def __setitem__(self, platform, key):
        self.set_key(platform, key)

    def set_key_secretly(self, platform):
        key = getpass.getpass("Enter the secret api key: ")
        self.set_key(platform, key)

    @final
    def __check_password(self, message="Please enter the password: "):
        pwd = getpass.getpass(message)
        if self.encode(pwd) != self.__password:
            print("Invalid password")
            return False
        return True

    def delete_key(self, platform):
        if self.__requires_pass and (not self.__check_password()):
            return
        if platform in self.__keys:
            del self.__keys[platform]
            print(f"API key for {platform} has been deleted.")
            self.save_to_file()
        else:
            print(f"No API key found for {platform}.")

    def change_password(self):
        old_pwd = getpass.getpass("Please enter the old password: ")
        if self.encode(old_pwd) == self.__password:
            while True:
                new_pwd = getpass.getpass("Please enter the new password: ")
                if new_pwd == "":
                    print("Password cannot be blank!")
                elif len(new_pwd) < 3:
                    print("Password length cannot be less than 3!")
                else:
                    self.__password = self.encode(new_pwd)
                    print("Password changed successfully")
                    self.save_to_file()
                    break
        else:
            print("Old password entered is incorrect!")

    def __getitem__(self, platform):
        return self.get(platform)

    def get(self, platform):
        if self.__requires_pass:
            if not self.__check_password():
                return None
        encoded_key = self.__keys.get(platform)
        if encoded_key:
            return self.decode(encoded_key)
        else:
            print(f"API key for {platform} not found!")
            return None

    def toggle_password_requirement(self, status=None):
        if status is not None:
            self.__requires_pass = status
        else:
            self.__requires_pass = not self.__requires_pass
        print(f"Password requirement set to: {self.__requires_pass}")

    def keys(self):
        return list(self.__keys.keys())

    def __str__(self):
        return f"Registered platforms: {self.keys()}"

    def __repr__(self):
        return self.__str__()

    # def __dict__(self):
    #     raise PermissionError("Access denied!")

    def dump(self):
        self.save_to_file()

    def use_api_key(self, platform, key_param_name="api_key"):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                key = self.get(platform)
                if key:
                    kwargs[key_param_name] = key
                    return func(*args, **kwargs)
                else:
                    return None

            return wrapper

        return decorator

    def __setattr__(self, name, value):
        """Prevent overwriting the `__check_password` method."""
        if name == '_API_Keys__check_password':
            raise PermissionError(f"Modification of {name} is restricted!")
        if name != "_API_Keys__secret_attr_00764354":
            if not hasattr(self, "_API_Keys__secret_attr_00764354"):
                if name == "_API_Keys__requires_pass":
                    if not self.__check_password():
                        raise PermissionError(f"Modification of {name} is restricted!")
                elif not hasattr(self, name):
                    raise PermissionError(f"Cannot add new attribute '{name}' to this object.")
        super().__setattr__(name, value)


def fetch_api_key(platform, requires_pass=True):
    api_keys = API_Keys(requires_pass=requires_pass)
    return api_keys.get(platform)


def add_api_key(platform):
    api_keys = API_Keys()
    api_keys.set_key_secretly(platform)
