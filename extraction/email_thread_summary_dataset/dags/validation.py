from airflow.sdk import task
from pathlib import Path
import pendulum
from pendulum import duration
import pandas as pd
import yaml

@task(retries=2, retry_delay=pendulum.duration(seconds=10))
def validation():
    """
    Validates the email thread summary dataset.

    This task checks for the following:
    1. Column names in the CSV file match the expected schema in the YAML file.
    2. Data types of each column in the CSV file match the expected schema in the YAML file.
    3. Nullability constraints are satisfied for each column.

    If any of the above checks fail, the task will raise a ValueError with a message detailing the
    mismatch.

    Returns a dictionary containing the total number of rows in the CSV file, the number of sampled rows
    checked, and a status of "success".
    """
    yml_file = "/opt/airflow/config/schema_expected.yml"
    csv_file = "/opt/airflow/sample_data/email_thread_details.csv"

    # read expected schema from yaml file
    with open(yml_file, "r") as f:
        schema_expected = yaml.safe_load(f)

    schema_columns = [col.get("name") for col in schema_expected.get("columns")]
    schema_dtype = {col.get("name"): col.get("type") for col in schema_expected.get("columns")}
    schema_nulls = {col.get("name"): col.get("nullable") for col in schema_expected.get("columns")}

    # read data from csv file
    df = pd.read_csv(csv_file)

    # load a random sample of 50 rows to infer schema (or full if smaller)
    random_sample = df.sample(n=min(50, len(df)), random_state=42)
    csv_columns = random_sample.columns.tolist()
    csv_dtype = random_sample.dtypes.apply(lambda x: x.name).to_dict()  # e.g., 'int64', 'object'
    csv_nulls = random_sample.isnull().sum().to_dict()

    # Validation 1: Check for column names
    missing_in_csv = set(schema_columns) - set(csv_columns)
    extra_in_actual = set(csv_columns) - set(schema_columns)
    if missing_in_csv or extra_in_actual:
        raise ValueError(
            "Column names mismatch! \n"
            f"Column Missing in csv but present in yml: {missing_in_csv} \n"
            f"Column Present in csv but missing in yml: {extra_in_actual}"
        )
    print("Column names validation successful!")

    # Validation 2: Check for data types — now strictly enforced
    # Mapping from your YAML types → expected pandas dtype
    yaml_to_pandas_dtype = {
        "integer": "int64",
        "varchar(255)": "object",   # string columns
        "text": "object",           # long text
        "timestamp": "object",      # we'll validate content separately if needed
    }

    type_errors = []
    for col in schema_columns:
        expected_yaml_type = schema_dtype[col]
        expected_pandas_dtype = yaml_to_pandas_dtype.get(expected_yaml_type)

        if expected_pandas_dtype is None:
            type_errors.append(f"Unknown type in YAML for column '{col}': {expected_yaml_type}")
            continue

        actual_pandas_dtype = csv_dtype[col]

        # Special strict check for integer
        if expected_yaml_type == "integer" and actual_pandas_dtype != "int64":
            type_errors.append(
                f"Type mismatch for '{col}': "
                f"expected 'integer' (int64), but got '{actual_pandas_dtype}'"
            )

        # String-based types (varchar, text, timestamp as string)
        elif expected_yaml_type in ["varchar(255)", "text", "timestamp"]:
            if actual_pandas_dtype != "object":
                type_errors.append(
                    f"Type mismatch for '{col}': "
                    f"expected '{expected_yaml_type}' (object/string), but got '{actual_pandas_dtype}'"
                )

            # Optional extra: validate timestamp format
            if expected_yaml_type == "timestamp":
                try:
                    pd.to_datetime(random_sample[col], errors="raise")
                except Exception:
                    type_errors.append(
                        f"Column '{col}' marked as 'timestamp' but contains invalid date/time values"
                    )

        #varchar length check
        if expected_yaml_type == "varchar(255)":
            max_length = random_sample[col].astype(str).str.len().max()
            if max_length > 255:
                type_errors.append(
                    f"Column '{col}' exceeds varchar(255) limit: longest value is {max_length} chars"
                )

    if type_errors:
        raise TypeError(
            "Data type validation failed!\n" + "\n".join(type_errors)
        )

    print("Data types validation successful!")

    # Validation 3: Check nullability
    null_errors = []
    for col in schema_columns:
        if not schema_nulls[col]:  # nullable: false
            if csv_nulls[col] > 0:
                null_errors.append(
                    f"Column '{col}' is not nullable but has {csv_nulls[col]} null value(s) in sample"
                )

    if null_errors:
        raise ValueError(
            "Nullability validation failed!\n" + "\n".join(null_errors)
        )

    print("Nullability validation successful!")

    print(f"All validations passed! Checked {len(random_sample)} sampled rows from total {len(df)}.")
    return {
        "total_rows": len(df),
        "sample_checked": len(random_sample),
        "status": "success"
    }