from base64 import b32encode


def int_to_bytes(x):
    return x.to_bytes((x.bit_length() + 7) // 8, "big")


def humanize(integer):
    """ Create a human-compatible ID from and integer """
    if integer is None:
        return None
    b32 = b32encode(int_to_bytes(integer))
    return b32.decode("ascii").replace("O", "8").replace("I", "9").rstrip("=")
