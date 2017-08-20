import aiomysql
import asyncio
import logging
import numbers
from collections.abc import Sequence
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def create_pool(loop, **kwargs):
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
    logging.info("SQL: {} {}".format(sql, params))
    async with __pool.get() as conn:
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
    logging.info("SQL: {} {}".format(sql, params))
    async with __pool.get() as conn:
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
    def __init__(cls, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)
        if not name == "Model":
            table_name = name
            mapping = {}
            fields = []
            primary_key = None
            for k, v in attr_dict.items():
                if isinstance(v, Field):
                    mapping[k] = v
                    v.storage_name = "_{}#{}".format(
                        type(v).__name__, k
                    )
                    if v.primary_key:
                        if primary_key:
                            raise RuntimeError("Duplicated primary key")
                        primary_key = k
                    else:
                        fields.append(k)
            cls.__table = table_name
            cls.__mapping = mapping
            cls.__fields = fields
            cls.__primary_key = primary_key


class Field:
    def __init__(self, column_type, primary_key, default):
        self.storage_name = None
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "<{}: {}>".format(
            self.__class__.__name__, self.__dict__
        )

    def validate(self, d_type, value):
        if not isinstance(value, d_type):
            raise ValueError("Type {} expected, got {}".format(
                d_type.__name__, value
            ))
        return value

    def __set__(self, instance, value):
        value = self.validate(instance, value)
        setattr(instance, self.storage_name, value)


class IntegerField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("bigint", primary_key, default)

    def validate(self, d_type, value):
        return super().validate(numbers.Integral, value)


class StringField(Field):
    def __init__(self, primary_key=False, default=None, ddl="Varchar(100)"):
        super().__init__(ddl, primary_key, default)

    def validate(self, d_type, value):
        return super().validate(str, value)


class BooleanField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("boolean", primary_key, default)

    def validate(self, d_type, value):
        return super().validate(bool, value)


class FloatField(Field):
    def __init__(self, primary_key=False, default=None):
        super().__init__("real", primary_key, default)

    def validate(self, d_type, value):
        return super().validate(numbers.Rational, value)


class Model(metaclass=ModelMeta):
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
        sql = " ".join(sql)+";"
        return sql, args
        rs = await select(sql, args)
        return [cls(**each) for each in rs]







# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(create_pool(
#         loop,
#         user="root",
#         password="solar",
#         db="indigo"
#     ))
#     # loop.run_until_complete(execute("insert into test values (%s);", ["Hello mysql"]))
#     print(loop.run_until_complete(select("select * from test;")))
#     __pool.close()
#     loop.run_until_complete(__pool.wait_closed())
