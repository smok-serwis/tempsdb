import os

from satella.json import read_json_from_file, write_json_to_file

cdef bint minijson_enabled

try:
    import minijson
    minijson_enabled = True
except ImportError:
    minijson_enabled = False

DEF METADATA_FILE_NAME = 'metadata.txt'
DEF METADATA_MINIJSON_FILE_NAME = 'metadata.minijson'


cdef dict read_meta_at(str path):
    cdef:
        bint exists_minijson = os.path.exists(os.path.join(path, METADATA_MINIJSON_FILE_NAME))
        bint exists_json = os.path.exists(os.path.join(path, METADATA_FILE_NAME))
        bytes data
    if exists_minijson:
        if not minijson_enabled:
            raise EnvironmentError('minijson required to open this series but not installed')
        with open(os.path.join(path, METADATA_MINIJSON_FILE_NAME), 'rb') as f_in:
            data = bytes(f_in.read())
            return minijson.loads(data)
    elif exists_json:
        return read_json_from_file(os.path.join(path, METADATA_FILE_NAME))
    else:
        return {}

cdef inline int write_meta_minijson(str path, dict meta):
    with open(os.path.join(path, METADATA_MINIJSON_FILE_NAME), 'wb') as f_out:
        f_out.write(minijson.dumps(meta))
    return 0

cdef inline int write_meta_json(str path, dict meta):
    write_json_to_file(os.path.join(path, METADATA_FILE_NAME), meta)
    return 0

cdef int write_meta_at(str path, dict meta):
    cdef:
        bint exists_minijson = os.path.exists(os.path.join(path, METADATA_MINIJSON_FILE_NAME))
        bint exists_json = os.path.exists(os.path.join(path, METADATA_FILE_NAME))
        bytes data
    if not exists_minijson and not exists_json:
        if minijson_enabled:
            return write_meta_minijson(path, meta)
        else:
            return write_meta_json(path, meta)
    elif exists_minijson and not minijson_enabled:
        raise EnvironmentError('minijson required to open this series but not installed')
    elif exists_minijson:
        return write_meta_minijson(path, meta)
    elif exists_json:
        return write_meta_json(path, meta)
    else:
        raise EnvironmentError('both metadata files exists!')
    return 0
