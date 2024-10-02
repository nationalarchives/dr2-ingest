import jsonschema
from jsonschema import validate

incoming_schema = {
    "type": "object",
    "properties": {
        "Series": {"type": "string"},
        "UUID": {"type": "string"},
        "TransferInitiatedDatetime": {"type": "string"}
    },
    "required": ["Series", "UUID", "TransferInitiatedDatetime"],
}
