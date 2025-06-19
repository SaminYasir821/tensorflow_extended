import apache_beam as beam
import re

with beam.Pipeline() as p:
    lines = p | beam.io.ReadFromText("input.txt")

    counts = (
        lines
        | "Split" >> beam.FlatMap(lambda x: re.findall(r"[A-Za-z']+", x))
        | "PairWithOne" >> beam.Map(lambda x: (x, 1))
        | "GroupAndSum" >> beam.CombinePerKey(sum)
        | "FormatResults" >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
        | "WriteOutput" >> beam.io.WriteToText("/home/samin96/vscode/output.txt")
    )
