# python -m src.db.hases


def create_hash(value):
    import hashlib

    return hashlib.md5(str(value).upper().encode('utf-8')).hexdigest()
