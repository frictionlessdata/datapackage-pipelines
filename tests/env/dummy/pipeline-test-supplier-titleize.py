from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

key = params['key']


def process_resources(_res_iter):
    for res in _res_iter:
        def process_res(_res):
            for line in _res:
                if key in line:
                    line[key] = line[key].title()
                    yield line
        yield process_res(res)

spew(datapackage, process_resources(res_iter))
