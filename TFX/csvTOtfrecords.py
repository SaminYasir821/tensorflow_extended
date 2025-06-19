""" Example module to convert CSV data to TFRecords
"""
import os
import csv
import tensorflow as tf
from tqdm import tqdm


def _bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value.encode()]))


def _int64_feature(value):
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


def clean_rows(row):
    # Ensure ZIP code exists
    if not row.get("ZIP code") or row["ZIP code"].strip() == "":
        row["ZIP code"] = "99999"
    return row


def convert_zipcode_to_int(zipcode):
    # Handle missing or invalid formats
    if not zipcode or zipcode.strip() in ["", "XXXXX", "XX", "0000X"]:
        return 99999  # fallback default ZIP code
    
    if isinstance(zipcode, str):
        zipcode = zipcode.replace("X", "0")  # Replace 'X' with '0' for numeric conversion

    try:
        return int(zipcode)
    except ValueError:
        return 99999  # fallback in case conversion still fails


# File paths
original_data_file = "/home/samin96/TFX/data/complaints.csv"
tfrecords_filename = "consumer-complaints.tfrecords"

# Create TFRecordWriter
tf_record_writer = tf.io.TFRecordWriter(tfrecords_filename)

# Read CSV and write TFRecords
with open(original_data_file, mode="r", encoding="utf-8") as csv_file:
    reader = csv.DictReader(csv_file, delimiter=",", quotechar='"')
    
    for row in tqdm(reader, desc="Converting CSV to TFRecord"):
        row = clean_rows(row)
        
        example = tf.train.Example(
            features=tf.train.Features(
                feature={
                    "product": _bytes_feature(row["Product"]),
                    "sub_product": _bytes_feature(row["Sub-product"]),
                    "issue": _bytes_feature(row["Issue"]),
                    "sub_issue": _bytes_feature(row["Sub-issue"]),
                    "state": _bytes_feature(row["State"]),
                    "zip_code": _int64_feature(convert_zipcode_to_int(row["ZIP code"])),
                    "company": _bytes_feature(row["Company"]),
                    "company_response": _bytes_feature(row["Company response to consumer"]),
                    "timely_response": _bytes_feature(row["Timely response?"]),
                    "consumer_disputed": _bytes_feature(row["Consumer disputed?"]),
                }
            )
        )

        tf_record_writer.write(example.SerializeToString())

tf_record_writer.close()
print(f"TFRecord file written to: {tfrecords_filename}")
