class SchemaError(Exception):
    """A required column is missing: the batch is rejected as a whole, not per record."""


def validate_schema(df, required, name="source"):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SchemaError(f"{name}: missing required column(s) {missing}")
