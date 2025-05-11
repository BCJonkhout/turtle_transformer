import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD
import sys
import re

# Define Namespaces
SAREF = Namespace("https://saref.etsi.org/core/")
EX = Namespace("http://example.com/")
EXDATA = Namespace("http://example.com/data/")

# Helper funcitons
def sanitize_for_uri(text):
    """Replaces characters unsafe for URIs with underscores."""
    text = re.sub(r'^DE_KN_', '', text)
    return re.sub(r'[^a-zA-Z0-9_]', '_', text)


def get_feature_of_interest_uri(column_name):
    """Extracts the building/location part for FeatureOfInterest URI."""
    match = re.match(r"DE_KN_([a-zA-Z]+[0-9]+)(_|$)", column_name)
    if match:
        return EXDATA[f"Location_{match.group(1)}"]
    return None  # Or a default FoI


def get_sensor_uri(column_name):
    """Creates a Sensor URI from the column name."""
    sanitized_name = sanitize_for_uri(column_name)
    return EXDATA[f"Sensor_{sanitized_name}"]


def get_property_uri(column_name):
    """Creates a Property URI from the column name (simplified)."""
    # Extracts the part after the building identifier
    match = re.match(r"DE_KN_[a-zA-Z]+[0-9]+_(.+)", column_name)
    prop_name = sanitize_for_uri(match.group(1)) if match else sanitize_for_uri(column_name)
    # Add a suffix to clarify it's a property type
    return EX[f"Property_{prop_name}"]


# main logic
def transform_csv_to_rdf(csv_filepath, output_filepath="graph.ttl"):
    """
    Reads the CSV, transforms it to SAREF RDF, and saves it as Turtle.
    """

    # Create RDF Graph
    g = Graph()
    g.bind("saref", SAREF)
    g.bind("ex", EX)
    g.bind("exdata", EXDATA)
    g.bind("xsd", XSD)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)


    df_header = pd.read_csv(csv_filepath, nrows=0)
    all_columns = df_header.columns.tolist()

    # identify potential measurement columns
    measurement_columns = [
        col for col in all_columns
        if '_' in col and col not in ['utc_timestamp', 'cet_cest_timestamp', 'interpolated']
    ]

    defined_sensors = set()
    defined_properties = set()
    defined_fois = set()

    print("Sensors etc....")
    for col in measurement_columns:
        sensor_uri = get_sensor_uri(col)
        prop_uri = get_property_uri(col)
        foi_uri = get_feature_of_interest_uri(col)

        if sensor_uri not in defined_sensors:
            g.add((sensor_uri, RDF.type, SAREF.Sensor))
            g.add((sensor_uri, RDFS.label, Literal(f"Sensor for {col}")))
            # link sensor to the property it measures
            g.add((sensor_uri, SAREF.measuresProperty, prop_uri))
            defined_sensors.add(sensor_uri)

        if prop_uri not in defined_properties:
            g.add((prop_uri, RDF.type, SAREF.Property))
            g.add((prop_uri, RDFS.label, Literal(f"Property measured by {col}")))
            defined_properties.add(prop_uri)

        if foi_uri and foi_uri not in defined_fois:
            g.add((foi_uri, RDF.type, SAREF.FeatureOfInterest))
            # extract a better label
            foi_label = foi_uri.split('/')[-1].replace('Location_', '').replace('_', ' ')
            g.add((foi_uri, RDFS.label, Literal(f"Location {foi_label}")))
            defined_fois.add(foi_uri)

    print(f"Defined {len(defined_sensors)} sensors, {len(defined_properties)} properties, {len(defined_fois)} foi.")

    df = pd.read_csv(csv_filepath)

    for index, row in df.iterrows():
        try:
            timestamp_str = row['utc_timestamp']
            timestamp_literal = Literal(timestamp_str, datatype=XSD.dateTime)

            # Parse the interpolated column
            interpolated_sensors_in_row = set()
            if pd.notna(row['interpolated']) and row['interpolated'].lower() != 'nan':
                interpolated_cols = [s.strip() for s in row['interpolated'].split('|')]
                for icol in interpolated_cols:
                    interpolated_sensors_in_row.add(get_sensor_uri(icol))

            for col in measurement_columns:
                sensor_uri = get_sensor_uri(col)

                if sensor_uri in interpolated_sensors_in_row:
                    continue

                value = row[col]
                if pd.notna(value):
                    try:
                        numeric_value = float(value)
                        value_literal = Literal(numeric_value, datatype=XSD.float)

                        prop_uri = get_property_uri(col)
                        foi_uri = get_feature_of_interest_uri(col)

                        obs_uri = EXDATA[f"Observation_{sanitize_for_uri(col)}_{timestamp_str}"]

                        # Add Observation triples
                        g.add((obs_uri, RDF.type, SAREF.Observation))
                        g.add((obs_uri, SAREF.hasTimestamp, timestamp_literal))
                        g.add((obs_uri, SAREF.hasValue, value_literal))
                        g.add((obs_uri, SAREF.madeBySensor, sensor_uri))
                        g.add((obs_uri, SAREF.relatesToProperty, prop_uri))
                        if foi_uri:
                            g.add((obs_uri, SAREF.isAbout, foi_uri))

                    except ValueError:
                        # print(f"Skipping non-numeric value in row {index}, col {col}: {value}")
                        pass
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            continue

    try:
        g.serialize(destination=output_filepath, format="turtle")
        print("#### Finish")
    except Exception as e:
        print(f"Error serializing graph: {e}")
        sys.exit(1)

if __name__ == "__main__":
    csv_file = sys.argv[1]
    transform_csv_to_rdf(csv_file)