from icedb.log import IceLogIO, Schema, SchemaConflictException

log = IceLogIO("dan-mbp")

sch = Schema()
added = sch.accumulate(["col_a"], ["VARCHAR"])
assert added

added = sch.accumulate(["col_b"], ["BIGINT"])
assert added

added = sch.accumulate(["col_a"], ["VARCHAR"])
assert added is False

try:
    added = sch.accumulate(["col_b"], ["VARCHAR"])
    raise Exception("Did not throw schema conflict exception error")
except SchemaConflictException as e:
    pass

print("passed!")