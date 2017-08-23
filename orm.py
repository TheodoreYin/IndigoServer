import aiomysql
import logging
import numbers
from collections.abc import Sequence, Iterator
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def create_pool(loop, **kwargs):
    """
    create connection pool for mysql connections
    using global variable __pool to access pool
    """
    logging.info("Create database connection pool...")
    global __pool
    __pool = await aiomysql.create_pool(
        maxsize=kwargs.get("maxsize", 100),
        loop=loop,
        host=kwargs.get("host", "127.0.0.1"),
        port=kwargs.get("port", 3306),
        user=kwargs["user"],
        password=kwargs["password"],
        db=kwargs["db"],
        charset=kwargs.get("charset", "utf8")
    )
    logging.info("Connect successful")

async def select(sql, params=[], size=None):
    """
    Wrap SELECT and retrurn a list of selected rows
    """
    logging.info("SQL: {} {}".format(sql, params))
    async with __pool.acquire() as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace("?", "%s"), params)
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info("rows returned: {}".format(len(rs)))
        return rs

async def execute(sql, params=[]):
    """
    wrap INSERT UPDATE DELETE and return affected row number
    """
    logging.info("SQL: {} {}".format(sql, params))
    async with __pool.acquire() as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        try:
            await cur.execute(sql.replace("?", "%s"), params)
            affected = cur.rowcount
            await conn.commit()
        except BaseException:
            logging.info("EXCEPTION OCCURED, ROLLBACK")
            await conn.rollback()
            raise
        logging.info("rows affected: {}".format(affected))
        return affected


class ModelMeta(type):
    """
    Metaclass of building Model
    add table infomation to Model
    discriptor's storage_name is also assigned here
    """
    def __init__(cls, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        if not name == "Model":
            table_name = name
            default_mapping = {}
            fields = []
            primary_key = None
            for k, v in attr_dict.items():
                if isinstance(v, Field):
                    v.storage_name = "_{}#{}".format(
                        type(v).__name__, k
                    )
                    fields.append(k)
                    if v.primary_key:
                        if primary_key:
                            raise RuntimeError("Duplicated primary key")
                        primary_key = k
                    if isinstance(v.default, Iterator):
                        default_mapping[k] = v.default
            cls.__table__ = table_name
            cls.__fields__ = fields
            cls.__primary_key__ = primary_key
            cls.__default__ = default_mapping


class Field:
    """
    Field base class
    A discriptor that validate specific field and raise error
    """
    def __init__(self, column_type, primary_key, default):
        self.storage_name = None
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "<{}: {}>".format(
            self.__class__.__name__, self.__dict__
        )

    def validate(self, instance, d_type, value):
        if not isinstance(value, d_type):
            raise TypeError("Type {} expected, got {}".format(
                d_type.__name__, type(value)
            ))
        return value

    def __set__(self, instance, value):
        value = self.validate(instance, None, value)
        setattr(instance, self.storage_name, value)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return getattr(instance, self.storage_name)


class IntegerField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("bigint", primary_key, default)

    def validate(self, instance, d_type, value):
        return super().validate(instance, numbers.Integral, value)


class StringField(Field):
    def __init__(self, primary_key=False, default=None, ddl="Varchar(100)"):
        super().__init__(ddl, primary_key, default)

    def validate(self, instance, d_type, value):
        return super().validate(instance, str, value)


class BooleanField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("boolean", primary_key, default)

    def validate(self, instance, d_type, value):
        return super().validate(instance, bool, value)


class FloatField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("real", primary_key, default)

    def validate(self, instance, d_type, value):
        return super().validate(instance, numbers.Rational, value)


class Model(metaclass=ModelMeta):
    """
    Model base class which maps table in database
    """
    def __init__(self, **kwargs):
        default_used = set(self.__default__.keys()) - set(kwargs.keys())
        for attr in set(self.__fields__) & set(kwargs.keys()):
            setattr(self, attr, kwargs.pop(attr))
        for attr in default_used:
            setattr(self, attr, next(self.__default__[attr]))
        if len(kwargs):
            raise AttributeError("Unused attribute for type {}: {}".format(
                self.__class__.__name__, kwargs
            ))

    @classmethod
    async def get(cls, **kwargs):
        sql = ["select * from `{}`".format(cls.__name__)]
        args = []
        orderby = kwargs.pop("orderby", None)
        limit = kwargs.pop("limit", None)
        if len(kwargs):
            sql.append("where")
            argfmt = []
            for k, v in kwargs.items():
                argfmt.append("`?`=?")
                args.append(k)
                args.append(v)
            sql.append(" and ".join(argfmt))
        if orderby:
            sql.append("order by `?`")
            args.append(orderby)
        if limit:
            sql.append("limit")
            if isinstance(limit, int):
                sql.append("?")
                args.append(limit)
            elif isinstance(limit, Sequence) and len(limit) == 2:
                sql.append("?,?")
                args.extend(list(limit))
            else:
                raise ValueError("limit must be an integer or binary sequence")
        sql = " ".join(sql) + ";"
        rs = await select(sql, args)
        return [cls(**each) for each in rs]

    @staticmethod
    def create_args_string(num):
        return ", ".join(["?"] * num)

    @staticmethod
    def create_kwargs_string(keys):
        return ", ".join(["`{}`=?".format(key) for key in keys])

    async def insert(self):
        sql = "insert into `{}` values ({});".format(
            self.__table__, self.create_args_string(len(self.__fields__))
        )
        args = [getattr(self, name) for name in self.__fields__]
        if await execute(sql, args) < 1:
            logging.warn("Failed to insert row. Primary key: {}".format(
                getattr(self, self.__primary_key__)
            ))

    async def update(self):
        sql = "update `{}` set {} where `{}`=?;".format(
            self.__table__,
            self.create_kwargs_string(self.__fields__),
            self.__primary_key__
        )
        args = [getattr(self, name) for name in self.__fields__]
        args.append(getattr(self, self.__primary_key__))
        if await execute(sql, args) < 1:
            logging.warn("Failed to update row. Primary key: {}".format(
                getattr(self, self.__primary_key__)
            ))

    async def delete(self):
        sql = "delete from `{}` where `{}`=?".format(
            self.__table__, self.__primary_key__
        )
        args = [getattr(self, self.__primary_key__)]
        if await execute(sql, args) < 1:
            logging.warn("Failed to delete row. Primary key: {}".format(
                getattr(self, self.__primary_key__)
            ))

    
from itertools import count
import asyncio
class Test(Model):
    name = StringField()
    id = IntegerField(primary_key=True, default=count(1))


def unit_test():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_pool(
        loop, user="root", password="solar", db="indigo"))
    a = Test(name="I finished the orm!")
    loop.run_until_complete(a.insert())
    res = loop.run_until_complete(Test.get())
    print([(x.id, x.name) for x in res])
    a.name = "I know I can do it"
    loop.run_until_complete(a.update())
    res = loop.run_until_complete(Test.get())
    print([(x.id, x.name) for x in res])
    loop.run_until_complete(a.delete())
    res = loop.run_until_complete(Test.get())
    print([(x.id, x.name) for x in res])

if __name__ == "__main__":
    unit_test()
 
