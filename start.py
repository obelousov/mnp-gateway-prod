import xmlschema
from fastapi import FastAPI

MNP_SCHEMA_PATH = "mnp_schema.xsd"

def init_schema(app: FastAPI):
    """Initialize and attach the MNP XML schema to the FastAPI app state."""
    app.state.mnp_schema = xmlschema.XMLSchema(MNP_SCHEMA_PATH)
