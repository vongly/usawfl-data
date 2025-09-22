import dlt
from dlt.sources import incremental

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

'''
    use 'SystemModstamp' as incremental field
'''


class SalesforceResource:
    def __init__(
            self,
            object_name,
            api_call_session,
            incremental_attribute=None,
        ):

        self.api_call_session = api_call_session
        self.object_name = object_name
        self.incremental_obj = incremental(incremental_attribute, initial_value=None) if incremental_attribute is not None else None
    
    def yield_query_results(self, incremental_obj=None):
        # Incremental Filter
        if incremental_obj:
            if incremental_obj.last_value:
                incremental_string = incremental_obj.last_value
            else:
                incremental_string = None
        else:
            incremental_string = None

        yield from self.api_call_session.yield_records(object_name=self.object_name, incremental_string=incremental_string)


    def create_resource(self):
        @dlt.resource(name=self.object_name, table_name=self.object_name, write_disposition='append', primary_key=None)
        def my_resource(incremental_obj=self.incremental_obj):
            # primary_key=None -> to record history of slow changing fields
            yield self.yield_query_results(incremental_obj=incremental_obj)
        return my_resource()

