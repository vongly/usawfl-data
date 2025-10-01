import dlt
from dlt.sources import incremental

'''
    use 'SystemModstamp' as incremental field
'''


class PostgresResource:
    def __init__(
            self,
            schema,
            table,
            call,
            incremental_attribute=None,
            write_disposition='append',
            **kwargs,
        ):

        self.call = call
        self.schema = schema
        self.table = table

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = incremental(incremental_attribute, initial_value=None) if incremental_attribute is not None else None
        self.write_disposition = write_disposition

    def yield_query_results(self, incremental_obj=None):
        # Incremental Filter -> requires incremental attribute
        if self.incremental_attribute:
            if incremental_obj:
                if incremental_obj.last_value:
                    incremental_string = incremental_obj.last_value
                else:
                    incremental_string = None
            else:
                incremental_string = None
        else:
            incremental_string = None

        yield from self.call.yield_records(
            schema=self.schema,
            table=self.table,
            incremental_attribute=self.incremental_attribute,
            incremental_string=incremental_string,
        )

    def create_resource(self):

        @dlt.resource(
            name=self.table,
            table_name=self.table,
            write_disposition=self.write_disposition,
            primary_key=None,
        )
        def my_resource(incremental_obj=self.incremental_obj):
            # primary_key=None -> to record history of slow changing fields
            yield self.yield_query_results(incremental_obj=incremental_obj)
        return my_resource()