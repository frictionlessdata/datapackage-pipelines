from datapackage_pipelines.wrapper import ingest, spew


if __name__ == "__main__":
    params, _, _ = ingest()

    datapackage = {
        'name': 'placeholder',
        'resources': params.get('resources')
    }

    spew(datapackage, [])
