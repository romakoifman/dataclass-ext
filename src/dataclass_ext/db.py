import logging
import time
from contextlib import contextmanager
from datetime import datetime

import boto3
import s3fs
from pynamodb.attributes import (
    UnicodeAttribute,
    UTCDateTimeAttribute,
    DiscriminatorAttribute,
    NumberAttribute,
    BooleanAttribute,
)
from pynamodb.models import Model

PROJECT_BUCKET = "isr-uae-first-project"


@contextmanager
def s3db(filename, flags="r"):
    fs = s3fs.S3FileSystem()
    fd = None
    try:
        fd = fs.open(f"{PROJECT_BUCKET}/{filename}", flags)
        yield fd
    finally:
        if fd:
            fd.close()


def s3glob(pathname):
    fs = s3fs.S3FileSystem()
    return fs.glob(f"{PROJECT_BUCKET}/{pathname}")


# Old s3 version

# id_values = "_".join(f"{k}={v}" for k, v in self.id().items())
#             with s3db(f"{self.__class__._collectionName()}/{id_values}", "w") as f:
#                 f.write(self._serialize())


#
# s3glob(f"{cls._collectionName()}/*")


# def store_object(data_class: type, data_object):
#     table = dynamodb_get_table(data_class)
#     item = {"id": data_object.id(), "data": data_object.serialize()}
#
#     # item.update(data_object.serialize())
#
#     if data_class.__keep_history__:
#         item["timestamp"] = datetime.now().isoformat()
#
#     table.put_item(Item=item)
#
#

# @cache
# def get_session():
#     session = boto3.Session()
#     conn_str = (
#             f"dynamodb://{session.get_credentials().access_key}:{session.get_credentials().secret_key} \
#             @dynamodb.{session.region_name}.amazonaws.com:443"
#             # + "?endpoint_url={endpoint_url}"
#     )
#
#     engine = create_engine(conn_str)
#     factory = sessionmaker(bind=engine)
#     return factory()


_DYNAMODB_TYPES = {
    int: NumberAttribute,
    str: UnicodeAttribute,
    float: NumberAttribute,
    bool: BooleanAttribute,
}


def get_db_class(data_class: type, create=True):
    boto_session = boto3.Session()

    class _DBClassDefinition:
        @classmethod
        def fill(cls, data_object):
            utc_now = datetime.utcnow()

            kwargs = {"id": data_object.id()}
            if data_class.is_complex():
                kwargs["data"] = data_object.serialize()
            else:
                kwargs.update(data_object.serialize())

            if data_class.__keep_history__:
                kwargs["history_timestamp"] = utc_now

            return cls(insert_timestamp=utc_now, **kwargs)

    _DBClassDefinition.id = UnicodeAttribute(hash_key=True, attr_name="id")
    _DBClassDefinition.insert_timestamp = UTCDateTimeAttribute(
        attr_name="insert_timestamp"
    )
    if data_class.is_complex():
        _DBClassDefinition.data = UnicodeAttribute(attr_name="data")
    else:
        for field in data_class.fields():
            attr_type = _DYNAMODB_TYPES.get(field.type)
            if not attr_type:
                raise Exception(f"Unsupported type {field.type} for field {field.name}")
            setattr(_DBClassDefinition, field.name, attr_type(attr_name=field.name))

    if data_class.__keep_history__:
        _DBClassDefinition.history_timestamp = UTCDateTimeAttribute(
            range_key=True, attr_name="history_timestamp"
        )

    class _DBClass(_DBClassDefinition, Model):
        class Meta:
            table_name = data_class.collection_name()
            region = boto_session.region_name

    if create and not _DBClass.exists():
        _DBClass.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    return _DBClass


def store_object(data_object):
    new_rec = get_db_class(data_object.__class__).fill(data_object)
    new_rec.save()


def _deserialize_db_results(data_class, query_results):
    res = []
    for v in query_results:
        if data_class.is_complex():
            res.append(data_class.deserialize(v.data))
        else:
            attrs = v.attribute_values
            attrs.pop("id")
            attrs.pop("insert_timestamp")
            attrs.pop("history_timestamp", None)
            res.append(data_class.deserialize(attrs))

    return res


def find_objects(data_class: type, **kwargs):
    model_class = get_db_class(data_class)
    where = None
    for n, v in kwargs.items():
        where &= getattr(model_class, "id").contains(f"{n}={v}")

    return _deserialize_db_results(data_class, model_class.scan(filter_condition=where))


def load_object(data_class: type, object_id: str):
    model_class = get_db_class(data_class)
    query_result = list(model_class.query(object_id))
    if not query_result:
        return None
    assert len(query_result) == 1
    return _deserialize_db_results(data_class, query_result)[0]


def find_history(data_object: object):
    data_class = data_object.__class__
    model_class = get_db_class(data_class)
    return _deserialize_db_results(data_class, model_class.query(data_object.id()))


def delete_all(data_class: type):
    model_class = get_db_class(data_class, False)
    if model_class.exists():
        model_class.delete_table()
    max_retries = 10
    retries = 0
    while model_class.exists():
        logging.info("waiting for deletion of table %s", model_class.Meta.table_name)
        time.sleep(1)
        retries += 1
        if retries > max_retries:
            logging.info("Table %s still exists", model_class.Meta.table_name)
            break
