import uuid


def get_new_guid():
    return uuid.uuid4()

def get_pdl_raw_hex_to_byte(raw_hex_string):
    return bytes.fromhex(raw_hex_string)
