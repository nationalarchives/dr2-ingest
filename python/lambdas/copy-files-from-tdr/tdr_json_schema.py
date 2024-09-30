import jsonschema
from jsonschema import validate

incoming_schema = {
    "type": "object",
    "properties": {
        "Series": {"type": "string"},
        "ConsignmentReference" : {"type": "string"},
        "UUID" : {"type": "string"},
    },
    "required": ["Series", "UUID", "ConsignmentReference"],
}
