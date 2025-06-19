import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

input = "input.txt"
output_file = "output.txt"

pipeOps = PipelineOptions()

with beam.Pipeline(options=pipeOps) as p:
    
    lines = p | ReadFromText(input)

    counts = (
        lines
        | "Split" >> beam.FlatMap(lambda x: re.findall(r"[A-Za-z']+", x))
        | "PairWithOne" >> beam.Map(lambda x: (x, 1))
        | "GroupAndSum" >> beam.CombinePerKey(sum)
    )


    def format_result(word_count):

        (word, count) = word_count
        return "{}: {}".format(word, count)
    
    output = counts | "Format" >> beam.Map(format_result)

    output | WriteToText(output_file)
