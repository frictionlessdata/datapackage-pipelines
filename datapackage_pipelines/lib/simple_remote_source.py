from datapackage_pipelines.wrapper import ingest, spew


params, _, _ = ingest()

datapackage = {
    'name': 'placeholder',
    'resources': params.get('resources')
}

spew(datapackage, [])
