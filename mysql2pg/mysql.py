import os
import typing


def mysql_params() -> dict[str, typing.Any]:
    params: dict[str, typing.Any] = {}
    try:
        params["db"] = os.environ["MYSQL_DATABASE"]
    except KeyError:
        pass
    try:
        params["host"] = os.environ["MYSQL_HOST"]
    except KeyError:
        pass
    try:
        params["password"] = os.environ["MYSQL_PASSWORD"]
    except KeyError:
        pass
    try:
        params["port"] = int(os.environ["MYSQL_PORT"])
    except KeyError:
        pass
    try:
        params["user"] = os.environ["MYSQL_USER"]
    except KeyError:
        pass
    return params
