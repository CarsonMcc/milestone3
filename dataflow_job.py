import apache_beam as beam
import json

PROJECT_ID = 'm3-design-380121'
TOPIC_NAME = 'smart_meter_topic'
TOPIC_NAME2 = 'smart_meter_topic2'

def to_fahrenheit(celsius):
    return celsius * 1.8 + 32

def to_psi(kpa):
    return kpa / 6.895

class ConvertToImperialUnits(beam.DoFn):
    def process(self, element):
        reading = json.loads(element.decode('utf-8'))
        if reading.get('pressure') is None or reading.get('temperature') is None:
            return
        reading['pressure'] = to_psi(reading['pressure'])
        reading['temperature'] = to_fahrenheit(reading['temperature'])
        return [json.dumps(reading).encode('utf-8')]

def run():
    pipeline_options = {
        'project': PROJECT_ID,
        'runner': 'DataflowRunner',
        'temp_location': 'gs://m3-design-380121-bucket/tmp',
        'staging_location': 'gs://m3-design-380121-bucket/staging',
        'streaming': True
    }
    
    with beam.Pipeline(options=beam.pipeline.PipelineOptions.from_dictionary(pipeline_options)) as pipeline:
        readings = (pipeline
                    | 'Read from PubSub' >> beam.io.gcp.pubsub.ReadFromPubSub(topic='projects/{}/topics/{}'.format(PROJECT_ID, TOPIC_NAME))
                    | 'Filter' >> beam.Filter(lambda x: x is not None)
                    | 'Convert' >> beam.ParDo(ConvertToImperialUnits())
                    | 'Write to PubSub' >> beam.io.gcp.pubsub.WriteToPubSub(topic='projects/{}/topics/{}'.format(PROJECT_ID, TOPIC_NAME2))
        )

if __name__ == '__main__':
    run()
