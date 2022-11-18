from math import trunc

def truncate(number, decimals):
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals == 0:
        return trunc(number)
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")

    factor = 10.0 ** decimals
    return trunc(number * factor) / factor