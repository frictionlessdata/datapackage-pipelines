# Datapackage Pipelines

[![Travis](https://img.shields.io/travis/frictionlessdata/datapackage-pipelines/master.svg)](https://travis-ci.org/frictionlessdata/datapackage-pipelines)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/datapackage-pipelines.svg?branch=master)](https://coveralls.io/r/frictionlessdata/datapackage-pipelines?branch=master)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/datapackage-pipelines.svg)

## The Basics

### What is it?

`datapackage-pipelines` is a framework for declarative stream-processing of tabular data. It is built upon the concepts and tooling of the Frictionless Data project.

### Pipelines

The basic concept in this framework is the pipeline.

A pipeline has a list of processing steps, and it generates a single *data package* as its output. Each step is executed in a _processor_ and consists of the following stages:

- **Modify the data package descriptor** - For example: add metadata, add or remove resources, change resources' data schema etc.
- **Process resources** - Each row of each resource is processed sequentially. The processor can drop rows, add new ones or modify their contents.
- **Return stats** - If necessary, the processor can report a dictionary of data which will be returned to the user when the pipeline execution terminates. This can be used, for example, for calculating quality measures for the processed data.

Not every processor needs to do all of these. In fact, you would often find each processing step doing only one of these.

### `pipeline-spec.yaml` file

Pipelines are defined in a declarative way, and not in code. One or more pipelines can be defined in a `pipeline-spec.yaml` file. This file specifies the list of processors (referenced by name) and the execution parameters for each of the processors.

Here's an example of a `pipeline-spec.yaml` file:

```yaml
worldbank-co2-emissions:
  title: CO2 emission data from the World Bank
  description: Data per year, provided in metric tons per capita.
  pipeline:
    -
      run: update_package
      parameters:
        name: 'co2-emissions'
        title: 'CO2 emissions (metric tons per capita)'
        homepage: 'http://worldbank.org/'
    -
      run: load
      parameters:
        from: "http://api.worldbank.org/v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel"
        name: 'global-data'
        format: xls
        headers: 4
    -
      run: set_types
      parameters:
         resources: global-data
         types:
           "[12][0-9]{3}":
              type: number
    -
      run: dump_to_zip
      parameters:
          out-file: co2-emissions-wb.zip
```

In this example we see one pipeline called `worldbank-co2-emissions`. Its pipeline consists of 4 steps:

- `metadata`: This is a library processor  (see below), which modifies the data-package's descriptor (in our case: the initial, empty descriptor) - adding `name`, `title` and other properties to the datapackage.
- `load`: This is another library processor, which loads data into the data-package.
  This resource has a `name` and a `from` property, pointing to the remote location of the data.
- `set_types`: This processor assigns data types to fields in the data. In this example, field headers looking like years will be assigned the `number` type.
- `dump_to_zip`: Create a zipped and validated datapackage with the provided file name.

### Mechanics

An important aspect of how the pipelines are run is the fact that data is passed in streams from one processor to another. If we get "technical" here, then each processor is run in its own dedicated process, where the datapackage is read from its `stdin` and output to its `stdout`. The important thing to note here is that no processor holds the entire data set at any point.

This limitation is by design - to keep the memory and disk requirements of each processor limited and independent of the dataset size.

### Quick Start

First off, create a `pipeline-spec.yaml` file in your current directory. You can take the above file if you just want to try it out.

Then, you can either install `datapackage-pipelines` locally - note that _Python 3.6_ or higher is required due to use of [Type Hinting](https://www.python.org/dev/peps/pep-0484/) and advanced `asyncio` use:

```shell
$ pip install datapackage-pipelines
```

You should now be able to use the `dpp` command:

```shell
$ dpp
Available Pipelines:
- ./worldbank-co2-emissions (*)

$ $ dpp run --verbose ./worldbank-co2-emissions
RUNNING ./worldbank-co2-emissions
Collecting dependencies
Running async task
Waiting for completion
Async task starting
Searching for existing caches
Building process chain:
- update_package
- load
- set_types
- dump_to_zip
- (sink)
DONE /Users/adam/code/dhq/specstore/dpp_repo/datapackage_pipelines/specs/../lib/update_package.py
load: DEBUG   :Starting new HTTP connection (1): api.worldbank.org:80
load: DEBUG   :http://api.worldbank.org:80 "GET /v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel HTTP/1.1" 200 308736
load: DEBUG   :http://api.worldbank.org:80 "GET /v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel HTTP/1.1" 200 308736
load: DEBUG   :Starting new HTTP connection (1): api.worldbank.org:80
load: DEBUG   :http://api.worldbank.org:80 "GET /v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel HTTP/1.1" 200 308736
load: DEBUG   :http://api.worldbank.org:80 "GET /v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel HTTP/1.1" 200 308736
set_types: INFO    :(<dataflows.processors.set_type.set_type object at 0x10a5c79b0>,)
load: INFO    :Processed 264 rows
set_types: INFO    :Processed 264 rows
DONE /Users/adam/code/dhq/specstore/dpp_repo/datapackage_pipelines/specs/../lib/load.py
DONE /Users/adam/code/dhq/specstore/dpp_repo/datapackage_pipelines/specs/../lib/set_types.py
dump_to_zip: INFO    :Processed 264 rows
DONE /Users/adam/code/dhq/specstore/dpp_repo/datapackage_pipelines/manager/../lib/internal/sink.py
DONE /Users/adam/code/dhq/specstore/dpp_repo/datapackage_pipelines/specs/../lib/dump_to_zip.py
DONE V ./worldbank-co2-emissions {'bytes': 692741, 'count_of_rows': 264, 'dataset_name': 'co2-emissions', 'hash': '4dd18effcdfbf5fc267221b4ffc28fa4'}
INFO    :RESULTS:
INFO    :SUCCESS: ./worldbank-co2-emissions {'bytes': 692741, 'count_of_rows': 264, 'dataset_name': 'co2-emissions', 'hash': '4dd18effcdfbf5fc267221b4ffc28fa4'}
```

Alternatively, you could use our [Docker](https://www.docker.com/) image:

```shell
$ docker run -it -v `pwd`:/pipelines:rw \
        frictionlessdata/datapackage-pipelines
<available-pipelines>

$ docker run -it -v `pwd`:/pipelines:rw \
       frictionlessdata/datapackage-pipelines run ./worldbank-co2-emissions
<execution-logs>
```

### The Command Line Interface - `dpp`

Running a pipeline from the command line is done using the `dpp` tool.

Running `dpp` without any argument, will show the list of available pipelines. This is done by scanning the current directory and its subdirectories, searching for `pipeline-spec.yaml` files and extracting the list of pipeline specificiations described within.

Each pipeline has an identifier, composed of the path to the `pipeline-spec.yaml` file and the name of the pipeline, as defined within that description file.

In order to run a pipeline, you use `dpp run <pipeline-id>`.

You can also use `dpp run all` for running all pipelines and `dpp run dirty` to run the just the _dirty_ pipelines (more on that later on).

## Deeper look into pipelines

### Processor Resolution

As previously seen, processors are referenced by name.

This name is, in fact, the name of a Python script containing the processing code (minus the `.py` extension). When trying to find where is the actual code that needs to be executed, the processor resolver will search in these predefined locations:

- First of all, it will try to find a custom processor with that name in the directory of the `pipeline-spec.yaml` file.
  Processor names support the dot notation, so you could write `mycode.custom_processor` and it will try to find a processor named `custom_processor.py` in the `mycode` directory, in the same path as the pipeline spec file.
  For this specific resolving phase, if you would write `..custom_processor` it will try to find that processor in the parent directory of the pipeline spec file.
  (read on for instructions on how to write custom processors)
- In case the processor name looks like `myplugin.somename`, it will try to find a processor named `somename` in the `myplugin` plugin. That is - it will see if there's an installed plugin which is called `myplugin`, and if so, whether that plugin publishes a processor called `somename` (more on plugins below).
- If no processor was found until this point, it will try to search for this processor in the processor search path. The processor search path is taken from the environment variable `DPP_PROCESSOR_PATH`. Each of the `:` separated paths in the path is considered as a possible starting point for resolving the processor.
- Finally, it will try to find that processor in the Standard Processor Library which is bundled with this package.

### Excluding directories form scanning for pipeline specs

By default `.*` directories are excluded from scanning, you can add additional directory patterns for
exclusion by creating a `.dpp_spec_ignore` file at the project root. This file has similar syntax
to .gitignore and will exclude directories from scanning based on glob pattern matching.

For example, the following file will ignore `test*` directories including inside subdirectories
and `/docs` directory will only be ignored at the project root directory

```
test*
/docs
```

### Caching

By setting the `cached` property on a specific pipeline step to `True`, this step's output will be stored on disk (in the `.cache` directory, in the same location as the `pipeline-spec.yaml` file).

Rerunning the pipeline will make use of that cache, thus avoiding the execution of the cached step and its precursors.

Internally, a hash is calculated for each step in the pipeline - which is based on the processor's code, it parameters and the hash of its predecessor. If a cache file exists with exactly the same hash as a specific step, then we can remove it (and its predecessors) and use that cache file as an input to the pipeline

This way, the cache becomes invalid in case the code or execution parameters changed (either for the cached processor or in any of the preceding processors).

### Dirty tasks and keeping state

The cache hash is also used for seeing if a pipeline is "dirty". When a pipeline completes executing successfully, `dpp` stores the cache hash along with the pipeline id. If the stored hash is different than the currently calculated hash, it means that either the code or the execution parameters were modified, and that the pipeline needs to be re-run.

`dpp` works with two storage backends. For running locally, it uses a python _sqlite DB_ to store the current state of each running task, including the last result and cache hash. The state DB file is stored in a file named `.dpp.db` in the same directory that `dpp` is being run from.

For other installations, especially ones using the task scheduler, it is recommended to work with the _Redis_ backend. In order to enable the Redis connection, simply set the `DPP_REDIS_HOST` environment variable to point to a running Redis instance.

### Pipeline Dependencies

You can declare that a pipeline is dependent on another pipeline or datapackage. This dependency is considered when calculating the cache hashes of a pipeline, which in turn affect the validity of cache files and the "dirty" state:
- For pipeline dependencies, the hash of that pipeline is used in the calculation
- For datapackage dependencies, the `hash` property in the datapackage is used in the calculation

If the dependency is missing, then the pipeline is marked as 'unable to be executed'.

Declaring dependencies is done by a `dependencies` property to a pipeline definition in the `pipeline-spec.yaml` file.
This property should contain a list of dependencies, each one is an object with the following formats:
- A single key named `pipeline` whose value is the pipeline id to depend on
- A single key named `datapackage` whose value is the identifier (or URL) for the datapackage to depend on

Example:
```yaml
cat-vs-dog-populations:
  dependencies:
    -
      pipeline: ./geo/region-areal
    -
      datapackage: http://pets.net/data/dogs-per-region/datapackage.json
    -
      datapackage: http://pets.net/data/dogs-per-region
  ...
```

### Validating

Each processor's input is automatically validated for correctness:

- The datapackage is always validated before being passed to a processor, so there's no possibility for a processor to modify a datapackage in a way that renders it invalid.

- Data is not validated against its respective JSON Table Schema, unless explicitly requested by setting the `validate` flag to True in the step's info.
  This is done for two main reasons:

  - Performance wise, validating the data in every step is very CPU intensive
  - In some cases you modify the schema in one step and the data in another, so you would only like to validate the data once all the changes were made

  In any case, when using the `set_types` standard processor, it will validate and transform the input data with the new types..

### Dataflows integration

[Dataflows](https://github.com/datahq/dataflows) is the successor of datapackage-pipelines and provides a more
Pythonic interface to running pipelines. You can integrate dataflows within pipeline specs using the `flow` attribute
instead of `run`. For example, given the following flow file, saved under `my-flow.py`:

```
from dataflows import Flow, dump_to_path, load, update_package

def flow(parameters, datapackage, resources, stats):
    stats['multiplied_fields'] = 0

    def multiply(field, n):
        def step(row):
            row[field] = row[field] * n
            stats['multiplied_fields'] += 1
        return step

    return Flow(update_package(name='my-datapackage'),
                multiply('my-field', 2))
```

And a `pipeline-spec.yaml` in the same directory:

```
my-flow:
  pipeline:
  - run: load_resource
    parameters:
      url: http://example.com/my-datapackage/datapackage.json
      resource: my-resource
  - flow: my-flow
  - run: dump_to_path
```

You can run the pipeline using `dpp run my-flow`.

## The Standard Processor Library

A few built in processors are provided with the library.

### ***`update_package`***

Adds meta-data to the data-package.

_Parameters_:

Any allowed property (according to the [spec]([http://specs.frictionlessdata.io/data-packages/#metadata)) can be provided here.

*Example*:

```yaml
- run: update_package
  parameters:
    name: routes-to-mordor
    license: CC-BY-SA-4
    author: Frodo Baggins <frodo@shire.me>
    contributors:
      - samwise gamgee <samwise1992@yahoo.com>
```

### ***`update_resource`***

Adds meta-data to the resource.

_Parameters_:

- `resources`
  - A name of a resource to operate on
  - A regular expression matching resource names
  - A list of resource names
  - The index of the resource in the package
  - if omitted indicates operation should be done on all resources
- `metadata` - A dictionary containing any allowed property (according to the [spec]([https://frictionlessdata.io/specs/data-resource/#metadata)).

*Example*:

```yaml
- run: update_resource
  parameters:
    resources: ['resource1']
    metadata:
      path: 'new-path.csv'
```

### ***`load`***

Loads data into the package, infers the schema and optionally casts values.

_Parameters_:
- `from` - location of the data that is to be loaded. This can be either:
  - a local path (e.g. /path/to/the/data.csv)
  - a remote URL (e.g. https://path.to/the/data.csv)
  - Other supported links, based on the current support of schemes and formats in [tabulator](https://github.com/frictionlessdata/tabulator-py#schemes)
  - a local path or remote URL to a datapackage.json file (e.g. https://path.to/data_package/datapackage.json)
  - a reference to an environment variable containing the source location, in the form of `env://ENV_VAR`
  - a tuple containing (datapackage_descriptor, resources_iterator)
- `resources` - optional, relevant only if source points to a datapackage.json file or datapackage/resource tuple. Value should be one of the following:
  - Name of a single resource to load
  - A regular expression matching resource names to load
  - A list of resource names to load
  - 'None' indicates to load all resources
  - The index of the resource in the package
- `validate` - Should data be casted to the inferred data-types or not. Relevant only when not loading data from datapackage.
- other options - based on the loaded file, extra options (e.g. sheet for Excel files etc., see the link to tabulator above)

### ***`printer`***

Just prints whatever it sees. Good for debugging.

_Parameters_:
- `num_rows` - modify the number of rows to preview, printer will print multiple samples of this number of rows from different places in the stream
- `last_rows` - how many of the last rows in the stream to print. optional, defaults to the value of num_rows
- `fields` - optional, list of field names to preview
- `resources` - optional, allows to limit the printed resources, same semantics as load processor resources argument

### ***`set_types`***

Sets data types and type options to fields in streamed resources, and make sure that the data still validates with the new types.

This allows to make modifications to the existing table schema, and usually to the default schema from `stream_remote_resources`.

_Parameters_:

-  `resources` - Which resources to modify. Can be:

   - List of strings, interpreted as resource names to stream
   - String, interpreted as a regular expression to be used to match resource names

   If omitted, all resources in datapackage are streamed.

- `regex` - if set to `False` field names will be interpreted as strings not as regular expressions (`True` by default)
-  `types` - A map between field names and field definitions.
   - _field name_ is either simply the name of a field, or a regular expression matching multiple fields.
   - _field definition_ is an object adhering to the [JSON Table Schema spec](http://specs.frictionlessdata.io/table-schema/). You can use `null` instead of an object to remove a field from the schema.


*Example*:

```yaml
- run: add_resources
  parameters:
    name: example-resource
    url: http://example.com/my-csv-file.csv
    encoding: "iso-8859-2"
- run: stream_remote_resources
- run: set_types
  parameters:
    resources: example-resource
    types:
      age:
        type: integer
      "yearly_score_[0-9]{4}":
        type: number
      "date of birth":
        type: date
        format: "%d/%m/%Y"
      "social security number": null
```

### ***`load_metadata`***

Loads metadata from an existing data-package.

_Parameters_:

Loads the metadata from the data package located at `url`.

All properties of the loaded datapackage will be copied (except the `resources`)

*Example*:

```yaml
- run: load_metadata
  parameters:
    url: http://example.com/my-datapackage/datapackage.json
```

### ***`load_resource`***

Loads a tabular resource from an existing data-package.

_Parameters_:

Loads the resource specified in the `resource` parameter from the data package located at `url`.
All properties of the loaded resource will be copied - `path` and `schema` included.

- `url` - a URL pointing to the datapackage in which the required resource resides

- `resource` - can be
   - List of strings, interpreted as resource names to load
   - String, interpreted as a regular expression to be used to match resource names
   - an integer, indicating the index of the resource in the data package (0-based)

- `limit-rows` - if provided, will limit the number of rows fetched from the source. Takes an integer value which specifies how many rows of the source to stream.

- `log-progress-rows` - if provided, will log the loading progress. Takes an integer value which specifies the number of rows interval at which to log the progress.

- `stream` - if provided and is set to false, then the resource will be added to the datapackage but not streamed.

- `resources` - can be used instead of `resource` property to support loading resources and modify the output resource metadata
    - Value is a dict containing mapping between source resource name to load and dict containing descriptor updates to apply to the loaded resource

- `required` - if provided and set to false, will not fail if datapackage is not available or resource is missing


*Example*:

```yaml
- run: load_resource
  parameters:
    url: http://example.com/my-datapackage/datapackage.json
    resource: my-resource
- run: load_resource
  parameters:
    url: http://example.com/my-other-datapackage/datapackage.json
    resource: 1
- run: load_resource
  parameters:
    url: http://example.com/my-datapackage/datapackage.json
    resources:
      my-resource:
        name: my-renamed-resource
        path: my-renamed-resource.csv
```


### ***`concatenate`***

Concatenates a number of streamed resources and converts them to a single resource.

_Parameters_:

- `sources` - Which resources to concatenate. Same semantics as `resources` in `stream_remote_resources`.

  If omitted, all resources in datapackage are concatenated.

  Resources to concatenate must appear in consecutive order within the data-package.

- `target` - Target resource to hold the concatenated data. Should define at least the following properties:

  - `name` - name of the resource
  - `path` - path in the data-package for this file.

  If omitted, the target resource will receive the name `concat` and will be saved at `data/concat.csv` in the datapackage.

- `fields` - Mapping of fields between the sources and the target, so that the keys are the _target_ field names, and values are lists of _source_ field names.

  This mapping is used to create the target resources schema.

  Note that the target field name is _always_ assumed to be mapped to itself.

*Example*:

```yaml
- run: concatenate
  parameters:
    target:
      name: multi-year-report
      path: data/multi-year-report.csv
    sources: 'report-year-20[0-9]{2}'
    fields:
      activity: []
      amount: ['2009_amount', 'Amount', 'AMOUNT [USD]', '$$$']
```

In this example we concatenate all resources that look like `report-year-<year>`, and output them to the `multi-year-report` resource.

The output contains two fields:

- `activity` , which is called `activity` in all sources
- `amount`, which has varying names in different resources (e.g. `Amount`, `2009_amount`, `amount` etc.)

### ***`join`***

Joins two streamed resources.

"Joining" in our case means taking the *target* resource, and adding fields to each of its rows by looking up data in the _source_ resource.

A special case for the join operation is when there is no target stream, and all unique rows from the source are used to create it.
This mode is called _deduplication_ mode - The target resource will be created and  deduplicated rows from the source will be added to it.

_Parameters_:

- `source` - information regarding the _source_ resource
  - `name` - name of the resource
  - `key` - One of
    - List of field names which should be used as the lookup key
    - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`)
  - `delete` - delete from data-package after joining (`False` by default)
- `target` - Target resource to hold the joined data. Should define at least the following properties:
  - `name` - as in `source`
  - `key` - as in `source`, or `null` for creating the target resource and performing _deduplication_.
- `fields` - mapping of fields from the source resource to the target resource.
  Keys should be field names in the target resource.
  Values can define two attributes:
  - `name` - field name in the source (by default is the same as the target field name)

  - `aggregate` - aggregation strategy (how to handle multiple _source_ rows with the same key). Can take the following options:
    - `sum` - summarise aggregated values.
      For numeric values it's the arithmetic sum, for strings the concatenation of strings and for other types will error.

    - `avg` - calculate the average of aggregated values.

      For numeric values it's the arithmetic average and for other types will err.

    - `max` - calculate the maximum of aggregated values.

      For numeric values it's the arithmetic maximum, for strings the dictionary maximum and for other types will error.

    - `min` - calculate the minimum of aggregated values.

      For numeric values it's the arithmetic minimum, for strings the dictionary minimum and for other types will error.

    - `first` - take the first value encountered

    - `last` - take the last value encountered

    - `count` - count the number of occurrences of a specific key
      For this method, specifying `name` is not required. In case it is specified, `count` will count the number of non-null values for that source field.

    - `counters` - count the number of occurrences of distinct values
      Will return an array of 2-tuples of the form `[value, count-of-value]`.

    - `set` - collect all distinct values of the aggregated field, unordered

    - `array` - collect all values of the aggregated field, in order of appearance

    - `any` - pick any value.

    By default, `aggregate` takes the `any` value.

  If neither `name` or `aggregate` need to be specified, the mapping can map to the empty object `{}` or to `null`.
- `full`  - Boolean,
  - If `True` (the default), failed lookups in the source will result in "null" values at the source.
  - if `False`, failed lookups in the source will result in dropping the row from the target.

_Important: the "source" resource **must** appear before the "target" resource in the data-package._

*Examples*:

```yaml
- run: join
  parameters:
    source:
      name: world_population
      key: ["country_code"]
      delete: yes
    target:
      name: country_gdp_2015
      key: ["CC"]
    fields:
      population:
        name: "census_2015"
    full: true
```

The above example aims to create a package containing the GDP and Population of each country in the world.

We have one resource (`world_population`) with data that looks like:

| country_code | country_name   | census_2000 | census_2015 |
| ------------ | -------------- | ----------- | ----------- |
| UK           | United Kingdom | 58857004    | 64715810    |
| ...          |                |             |             |

And another resource (`country_gdp_2015`) with data that looks like:

| CC   | GDP (£m) | Net Debt (£m) |
| ---- | -------- | ------------- |
| UK   | 1832318  | 1606600       |
| ...  |          |               |

The `join` command will match rows in both datasets based on the `country_code` / `CC` fields, and then copying the value in the `census_2015` field into a new `population` field.

The resulting data package will have the `world_population` resource removed and the `country_gdp_2015` resource looking like:

| CC   | GDP (£m) | Net Debt (£m) | population |
| ---- | -------- | ------------- | ---------- |
| UK   | 1832318  | 1606600       | 64715810   |
| ...  |          |               |            |



A more complex example:

```yaml
- run: join
  parameters:
    source:
      name: screen_actor_salaries
      key: "{production} ({year})"
    target:
      name: mgm_movies
      key: "{title}"
    fields:
      num_actors:
        aggregate: 'count'
      average_salary:
        name: salary
        aggregate: 'avg'
      total_salaries:
        name: salary
        aggregate: 'sum'
    full: false
```

This example aims to analyse salaries for screen actors in the MGM studios.

Once more, we have one resource (`screen_actor_salaries`) with data that looks like:

| year | production                  | actor             | salary   |
| ---- | --------------------------- | ----------------- | -------- |
| 2016 | Vertigo 2                   | Mr. T             | 15000000 |
| 2016 | Vertigo 2                   | Robert Downey Jr. | 7000000  |
| 2015 | The Fall - Resurrection     | Jeniffer Lawrence | 18000000 |
| 2015 | Alf - The Return to Melmack | The Rock          | 12000000 |
| ...  |                             |                   |          |

And another resource (`mgm_movies`) with data that looks like:

| title                     | director      | producer     |
| ------------------------- | ------------- | ------------ |
| Vertigo 2 (2016)          | Lindsay Lohan | Lee Ka Shing |
| iRobot - The Movie (2018) | Mr. T         | Mr. T        |
| ...                       |               |              |

The `join` command will match rows in both datasets based on the movie name and production year. Notice how we overcome incompatible fields by using different key patterns.

The resulting dataset could look like:

| title            | director      | producer     | num_actors | average_salary | total_salaries |
| ---------------- | ------------- | ------------ | ---------- | -------------- | -------------- |
| Vertigo 2 (2016) | Lindsay Lohan | Lee Ka Shing | 2          | 11000000       | 22000000       |
| ...              |               |              |            |                |                |


### ***`filter`***

Filter streamed resources.

`filter` accepts equality and inequality conditions and tests each row in the selected resources. If none of the conditions validate, the row will be discarded.

_Parameters_:

- `resources` - Which resources to apply the filter on. Same semantics as `resources` in `stream_remote_resources`.
- `in` - Mapping of keys to values which translate to `row[key] == value` conditions
- `out` - Mapping of keys to values which translate to `row[key] != value` conditions

Both `in` and `out` should be a list of objects. However, `out` should only ever have one element.

*Examples*:

Filtering just American and European countries, leaving out countries whose main language is English:
```yaml
- run: filter
  parameters:
    resources: world_population
    in:
      - continent: america
      - continent: europe
- run: filter
  parameters:
    resources: world_population
    out:
      - language: english
```
To filter `out` by multiple values, you need multiple filter processors, not multiple `out` elements. Otherwise some condition will always validate and no rows will be discareded:

```
- run: filter
  parameters:
    resources: world_population
    out:
      - language: english
- run: filter
  parameters:
    resources: world_population
    out:
      - language: swedish
```

### ***`sort`***

Sort streamed resources by key.

`sort` accepts a list of resources and a key (as a Python format string on row fields).
It will output the rows for each resource, sorted according to the key (in ascending order by default).

_Parameters_:

- `resources` - Which resources to sort. Same semantics as `resources` in `stream_remote_resources`.
- `sort-by` - String, which would be interpreted as a Python format string used to form the key (e.g. `{<field_name_1>}:{field_name_2}`)
- `reverse` - Optional boolean, if set to true - sorts in reverse order

*Examples*:

Filtering just American and European countries, leaving out countries whose main language is English:
```yaml
- run: sort
  parameters:
    resources: world_population
    sort-by: "{country_name}"
```

### ***`duplicate`***

Duplicate a resource.

`duplicate` accepts the name of a single resource in the datapackage.
It will then duplicate it in the output datapackage, with a different name and path.
The duplicated resource will appear immediately after its original.

_Parameters_:

- `source` - Which resources to duplicate. The name of the resource.
- `target-name` - Name of the new, duplicated resource.
- `target-path` - Path for the new, duplicated resource.

*Examples*:

Filtering just American and European countries, leaving out countries whose main language is English:
```yaml
- run: duplicate
  parameters:
    source: original-resource
    target-name: copy-of-resource
    target-path: data/duplicate.csv
```


### ***`delete_fields`***

Delete fields (columns) from streamed resources

`delete_fields` accepts a list of resources and list of fields to remove

_Note: if multiple resources provided, all of them should contain all fields to delete_

_Parameters_:

- `resources` - Which resources to delete columns from. Same semantics as `resources` in `stream_remote_resources`.
- `fields` - List of field (column) names to be removed (exact names or regular expressions for matching field names)
- `regex` - if set to `False` field names will be interpreted as strings not as regular expressions (`True` by default)

*Examples*:

Deleting `country_name` and `census_2000` columns from `world_population` resource:
```yaml
- run: delete_fields
  parameters:
    resources: world_population
    fields:
      - country_name
      - census_2000
```

### ***`add_computed_field`***

Add field(s) to streamed resources

`add_computed_field` accepts a list of resources and fields to add to existing resource. It will output the rows for each resource with new field(s) (columns) in it. `add_computed_field` allows to perform various operations before inserting value into targeted field.

_Parameters_:

- `resources` - Resources to add field. Same semantics as `resources` in `stream_remote_resources`.
- `fields` - List of operations to be performed on the targeted fields.
  - `operation`: operation to perform on values of pre-defined columns of the same row. available operation:
    - `constant` - add a constant value
    - `sum` - summed value for given columns in a row.
    - `avg` - average value from given columns in a row.
    - `min` - minimum value among given columns in a row.
    - `max` - maximum value among given columns in a row.
    - `multiply` - product of given columns in a row.
    - `join` - joins two or more column values in a row.
    - `format` - Python format string used to form the value Eg:  `my name is {first_name}`.
  - `target` - name of the new field.
  - `source` - list of columns the operations should be performed on (Not required in case of `format` and `constant`).
  - `with` - String passed to `constant`, `format` or `join` operations
    - in `constant` - used as constant value
    - in `format` - used as Python format string with existing column values Eg: `{first_name} {last_name}`
    - in `join` - used as delimiter

*Examples*:

Following example adds 4 new field to `salaries` resource

```yaml
run: add_computed_field
parameters:
  resources: salaries
  fields:
    -
      operation: sum
      target: total
      source:
        - jan
        - feb
        - may
    -
      operation: avg
      target: average
      source:
        - jan
        - feb
        - may
    -
      operation: format
      target: full_name
      with: '{first_name} {last_name}'
    -
      operation: constant
      target: status
      with: single
```

We have one resource (`salaries`) with data that looks like:

| first_name | last_name | jan | feb | mar |
| ---------- | --------- | --- | --- | --- |
| John       | Doe       | 100 | 200 | 300 |
| ...        |           |     |     |     |

The resulting dataset could look like:

| first_name | last_name | last_name | jan | feb | mar | average | total | status |
| ---------- | --------- | --------- | --- | --- | --- | ------- | ----- | ------ |
| John       | Doe       | John Doe  | 100 | 200 | 300 | 200     | 600   | single |
| ...        |           |           |     |     |     |         |       |        |

### ***`find_replace`***

find and replace string or pattern from field(s) values

_Parameters_:

- `resources` - Resources to clean the field values. Same semantics as `resources` in `stream_remote_resources`

- `fields`- list of fields to replace values
  - `name` - name of the field to replace value
  - `patterns` - list of patterns to find and replace from field
    - `find` - String, interpreted as a regular expression to match field value
    - `replace` - String, interpreted as a regular expression to replace matched pattern

*Examples*:

Following example replaces field values using regular expression and exact string patterns

```yaml
run: find_replace
parameters:
  resources: dates
  fields:
    -
      name: year
      patterns:
        -
          find: ([0-9]{4})( \(\w+\))
          replace: \1
    -
      name: quarter
      patterns:
        -
          find: Q1
          replace: '03-31'
        -
          find: Q2
          replace: '06-31'
        -
          find: Q3
          replace: '09-30'
        -
          find: Q4
          replace: '12-31'
```

We have one resource (`dates`) with data that looks like:

|   year   |  quarter  |
| -------- | --------- |
| 2000 (1) | 2000-Q1   |
| ...      |           |

The resulting dataset could look like:

| year |  quarter   |
| ---- | ---------- |
| 2000 | 2000-03-31 |
| ...  |            |

### ***`unpivot`***

Unpivots, transposes tabular data so that there's only one record per row.

_Parameters_:

- `resources` - Resources to unpivot. Same semantics as `resources` in `stream_remote_resources`.
- `extraKeyFields` - List of target field definitions, each definition is an object containing at least these properties (unpivoted column values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `extraValueField` - Target field definition - an object containing at least these properties (unpivoted cell values will go here)
  - `name` - Name of the target field
  - `type` - Type of the target field
- `unpivot` - List of source field definitions, each definition is an object containing at least these properties
  - `name` - Either simply the name, or a regular expression matching the name of original field to unpivot.
  - `keys` - A Map between target field name and values for original field
    - Keys should be target field names from `extraKeyFields`
    - Values may be either simply the constant value to insert, or a regular expression matching the `name`.

_Examples_:

Following example will unpivot data into 3 new fields: `year`, `direction` and `amount`

```yaml
parameters:
  resources: balance
  extraKeyFields:
    -
      name: year
      type: integer
    -
      name: direction
      type: string
      constraints:
        enum:
          - In
          - Out
  extraValueField:
      name: amount
      type: number
  unpivot:
    -
      name: 2015 incomes
      keys:
        year: 2015
        direction: In
    -
      name: 2015 expenses
      keys:
        year: 2015
        direction: Out
    -
      name: 2016 incomes
      keys:
        year: 2016
        direction: In
    -
      name: 2016 expenses
      keys:
        year: 2016
        direction: Out
```

We have one resource (`balance`) with data that looks like:

| company | 2015 incomes | 2015 expenses | 2016 incomes | 2016 expenses |
| --------| ------------ | ------------- | ------------ | ------------- |
| Inc     | 1000         | 900           | 2000         | 1700          |
| Org     | 2000         | 800           | 3000         | 2000          |
| ...     |              |               |              |               |

The resulting dataset could look like:

| company | year | direction | amount |
| --------| ---- | --------- | ------ |
| Inc     | 2015 | In        | 1000   |
| Inc     | 2015 | Out       | 900    |
| Inc     | 2016 | In        | 2000   |
| Inc     | 2016 | Out       | 1700   |
| Org     | 2015 | In        | 2000   |
| Org     | 2015 | Out       | 800    |
| Org     | 2016 | In        | 3000   |
| Org     | 2016 | Out       | 2000   |
| ...     |      |           |        |

Similar result can be accomplished by defining regular expressions instead of constant values

```yaml
parameters:
  resources: balance
  extraKeyFields:
    -
      name: year
      type: integer
    -
      name: direction
      type: string
      constraints:
        enum:
          - In
          - Out
  extraValueField:
      name: amount
      type: number
  unpivot:
    -
      name: ([0-9]{4}) (\\w+)  # regex for original column
      keys:
        year: \\1  # First member of group from above
        direction: \\2  # Second member of group from above
```

### ***`dump_to_sql`***

Saves the datapackage to an SQL database.

_Parameters_:

- `engine` - Connection string for connecting to the SQL Database (URL syntax)
  Also supports `env://<environment-variable>`, which indicates that the connection string should be fetched from the indicated environment variable.
  If not specified, assumes a default of `env://DPP_DB_ENGINE`
- `tables` - Mapping between resources and DB tables. Keys are table names, values are objects with the following attributes:
  - `resource-name` - name of the resource that should be dumped to the table
  - `mode` - How data should be written to the DB.
    Possible values:
      - `rewrite` (the default) - rewrite the table, all previous data (if any) will be deleted.
      - `append` - write new rows without changing already existing data.
      - `update` - update the table based on a set of "update keys".
        For each new row, see if there already an existing row in the DB which can be updated (that is, an existing row
        with the same values in all of the update keys).
        If so - update the rest of the columns in the existing row. Otherwise - insert a new row to the DB.
  - `update_keys` - Only applicable for the `update` mode. A list of field names that should be used to check for row existence.
        If left unspecified, will use the schema's `primaryKey` as default.
  - `indexes` - TBD
- `updated_column` - Optional name of a column that will be added to the spewed data with boolean value
  - `true` - row was updated
  - `false` - row was inserted
- `updated_id_column` - Optional name of a column that will be added to the spewed data and contain the id of the updated row in DB.

### ***`dump_to_path`***
Saves the datapackage to a filesystem path.

_Parameters_:

- `out-path` - Name of the output path where `datapackage.json` will be stored.

  This path will be created if it doesn't exist, as well as internal data-package paths.

  If omitted, then `.` (the current directory) will be assumed.

- `force-format` - Specifies whether to force all output files to be generated with the same format
    - if `True` (the default), all resources will use the same format
    - if `False`, format will be deduced from the file extension. Resources with unknown extensions will be discarded.
- `format` - Specifies the type of output files to be generated (if `force-format` is true): `csv` (the default) or `json`
- `add-filehash-to-path`: Specifies whether to include file md5 hash into the resource path. Defaults to `False`. If `True` Embeds hash in path like so:
    - If original path is `path/to/the/file.ext`
    - Modified path will be `path/to/the/HASH/file.ext`
- `counters` - Specifies whether to count rows, bytes or md5 hash of the data and where it should be stored. An object with the following properties:
    - `datapackage-rowcount`: Where should a total row count of the datapackage be stored (default: `count_of_rows`)
    - `datapackage-bytes`: Where should a total byte count of the datapackage be stored (default: `bytes`)
    - `datapackage-hash`: Where should an md5 hash of the datapackage be stored (default: `hash`)
    - `resource-rowcount`: Where should a total row count of each resource be stored (default: `count_of_rows`)
    - `resource-bytes`: Where should a total byte count of each resource be stored (default: `bytes`)
    - `resource-hash`: Where should an md5 hash of each resource be stored (default: `hash`)
    Each of these attributes could be set to null in order to prevent the counting.
    Each property could be a dot-separated string, for storing the data inside a nested object (e.g. `stats.rowcount`)
- `pretty-descriptor`: Specifies how datapackage descriptor (`datapackage.json`) file will look like:
    - `False` (default) - descriptor will be written in one line.
    - `True` - descriptor will have indents and new lines for each key, so it becomes more human-readable.

### ***`dump_to_zip`***

Saves the datapackage to a zipped archive.

_Parameters_:

- `out-file` - Name of the output file where the zipped data will be stored
- `force-format` and `format` - Same as in `dump_to_path`
- `add-filehash-to-path` - Same as in `dump_to_path`
- `counters` - Same as in `dump_to_path`
- `pretty-descriptor` - Same as in `dump_to_path`

## Deprecated Processors

These processors will be removed in the next major version.

### ***`add_metadata`***

Alias for `update_package`, is kept for backward compatibility reasons.

### ***`add_resource`***

Adds a new external tabular resource to the data-package.

_Parameters_:

You should provide a `name` and `url` attributes, and other optional attributes as defined in the [spec]([http://specs.frictionlessdata.io/data-packages/#resource-information).

`url` indicates where the data for this resource resides. Later on, when `stream_remote_resources` runs, it will use the `url` (which is stored in the resource in the `dpp:streamedFrom` property) to read the data rows and push them into the pipeline.

Note that `url` also supports `env://<environment-variable>`, which indicates that the resource url should be fetched from the indicated environment variable.  This is useful in case you are supplying a string with sensitive information (such as an SQL connection string for streaming from a database table).

Parameters are basically arguments that are passed to a `tabulator.Stream` instance (see the [API](https://github.com/frictionlessdata/tabulator-py#api-reference)).
Other than those, you can pass a `constants` parameter which should be a mapping of headers to string values.
When used in conjunction with `stream_remote_resources`, these constant values will be added to each generated row
(as well as to the default schema).

You may also provide a schema here, or use the default schema generated by the `stream_remote_resources` processor.
In case `path` is specified, it will be used. If not, the `stream_remote_resources` processor will assign a `path` for you with a `csv` extension.

*Example*:

```yaml
- run: add_resource
  parameters:
    url: http://example.com/my-excel-file.xlsx
    sheet: 1
    headers: 2
- run: add_resource
  parameters:
    url: http://example.com/my-csv-file.csv
    encoding: "iso-8859-2"
```

### ***`stream_remote_resources`***

Converts external resources to streamed resources.

External resources are ones that link to a remote data source (url or file path), but are not processed by the pipeline and are kept as-is.

Streamed resources are ones that can be processed by the pipeline, and their output is saved as part of the resulting datapackage.

In case a resource has no schema, a default one is generated automatically here by creating a `string` field from each column in the data source.

_Parameters_:

- `resources` - Which resources to stream. Can be:

  - List of strings, interpreted as resource names to stream
  - String, interpreted as a regular expression to be used to match resource names

  If omitted, all resources in datapackage are streamed.

- `ignore-missing` - if true, then missing resources won't raise an error but will be treated as 'empty' (i.e. with zero rows).
  Resources with empty URLs will be treated the same (i.e. will generate an 'empty' resource).

- `limit-rows` - if provided, will limit the number of rows fetched from the source. Takes an integer value which specifies how many rows of the source to stream.

*Example*:

```yaml
- run: stream_remote_resources
  parameters:
    resources: ['2014-data', '2015-data']
- run: stream_remote_resources
  parameters:
    resources: '201[67]-data'
```

This processor also supports loading plain-text resources (e.g. html pages) and handling them as tabular data - split into rows with a single "data" column.
To enable this behavior, add the following attribute to the resource: `"format": "txt"`.

### ***`dump.to_sql`***

Alias for `dump_to_sql`, is kept for backward compatibility reasons.

### ***`dump.to_path`***

Saves the datapackage to a filesystem path.

_Parameters_:

- `out-path` - Name of the output path where `datapackage.json` will be stored.

  This path will be created if it doesn't exist, as well as internal data-package paths.

  If omitted, then `.` (the current directory) will be assumed.

- `force-format` - Specifies whether to force all output files to be generated with the same format
    - if `True` (the default), all resources will use the same format
    - if `False`, format will be deduced from the file extension. Resources with unknown extensions will be discarded.
- `format` - Specifies the type of output files to be generated (if `force-format` is true): `csv` (the default) or `json`
- `handle-non-tabular` - Specifies whether non tabular resources (i.e. resources without a `schema`) should be dumped as well to the resulting datapackage.
    (See note below for more details)
- `add-filehash-to-path`: Specifies whether to include file md5 hash into the resource path. Defaults to `False`. If `True` Embeds hash in path like so:
    - If original path is `path/to/the/file.ext`
    - Modified path will be `path/to/the/HASH/file.ext`
- `counters` - Specifies whether to count rows, bytes or md5 hash of the data and where it should be stored. An object with the following properties:
    - `datapackage-rowcount`: Where should a total row count of the datapackage be stored (default: `count_of_rows`)
    - `datapackage-bytes`: Where should a total byte count of the datapackage be stored (default: `bytes`)
    - `datapackage-hash`: Where should an md5 hash of the datapackage be stored (default: `hash`)
    - `resource-rowcount`: Where should a total row count of each resource be stored (default: `count_of_rows`)
    - `resource-bytes`: Where should a total byte count of each resource be stored (default: `bytes`)
    - `resource-hash`: Where should an md5 hash of each resource be stored (default: `hash`)
    Each of these attributes could be set to null in order to prevent the counting.
    Each property could be a dot-separated string, for storing the data inside a nested object (e.g. `stats.rowcount`)
- `pretty-descriptor`: Specifies how datapackage descriptor (`datapackage.json`) file will look like:
    - `False` (default) - descriptor will be written in one line.
    - `True` - descriptor will have indents and new lines for each key, so it becomes more human-readable.
- `file-formatters`: Specifies custom file format handlers. An object with mapping of format name to Python module and class name.
    - Allows to override the existing `csv` and `json` format handlers or add support for new formats.
    - Note that such changes may make the resulting datapackage incompatible with the frictionlessdata specs and may cause interoperability problems.
    - Example usage: [pipeline-spec.yaml](tests/cli/pipeline-spec.yaml) (under the `custom-formatters` pipeline), [XLSXFormat class](tests/cli/custom_formatters/xlsx_format.py)

### ***`dump.to_zip`***

Saves the datapackage to a zipped archive.

_Parameters_:

- `out-file` - Name of the output file where the zipped data will be stored
- `force-format` and `format` - Same as in `dump_to_path`
- `handle-non-tabular` - Same as in `dump_to_path`
- `add-filehash-to-path` - Same as in `dump_to_path`
- `counters` - Same as in `dump_to_path`
- `pretty-descriptor` - Same as in `dump_to_path`
- `file-formatters` - Same as in `dump_to_path`

#### *Note*

`dump.to_path` and `dump.to_zip` processors will handle non-tabular resources as well.
These resources must have both a `url` and `path` properties, and _must not_ contain a `schema` property.
In such cases, the file will be downloaded from the `url` and placed in the provided `path`.

## Custom Processors

It's quite reasonable that for any non-trivial processing task, you might encounter a problem that cannot be solved using the standard library processors.

For that you might need to write your own processor - here's how it's done.

There are two APIs for writing processors - the high level API and the low level API.

### High Level Processor API

The high-level API is quite useful for most processor kinds:

```python
from datapackage_pipelines.wrapper import process

def modify_datapackage(datapackage, parameters, stats):
    # Do something with datapackage
    return datapackage

def process_row(row, row_index,
                resource_descriptor, resource_index,
                parameters, stats):
    # Do something with row
    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)
```

The high level API consists of one method, `process` which takes two functions:

- `modify_datapackage` - which makes changes (if necessary) to the data-package descriptor, e.g. adds metadata, adds resources, modifies resources' schema etc.

  Can also be used for initialization code when needed.

  It has these arguments:

  - `datapackage` is the current data-package descriptor that needs to be modified.
    The modified data-package descriptor needs to be returned.
  - `parameters` is a dict containing the processor's parameters, as provided in the `pipeline-spec.yaml` file.
  - `stats` is a dict which should be modified in order to collect metrics and measurements in the process (e.g. validation checks, row count etc.)

- `process_row` - which modifies a single row in the stream. It receives these arguments:
  - `row` is a dictionary containing the row to process
  - `row_index` is the index of the row in the resource
  - `resource_descriptor` is the descriptor object of the current resource being processed
  - `resource_index` is the index of the resource in the data-package
  - `parameters` is a dict containing the processor's parameters, as provided in the `pipeline-spec.yaml` file.
  - `stats` is a dict which should be modified in order to collect metrics and measurements in the process (e.g. validation checks, row count etc.)

  and yields zero or more processed rows.

#### A few examples

```python
# Add license information
from datapackage_pipelines.wrapper import process

def modify_datapackage(datapackage, parameters, stats):
    datapackage['license'] = 'CC-BY-SA'
    return datapackage

process(modify_datapackage=modify_datapackage)
```

```python
# Add new column with constant value to first resource
# Column name and value are taken from the processor's parameters
from datapackage_pipelines.wrapper import process

def modify_datapackage(datapackage, parameters, stats):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': parameters['column-name'],
      'type': 'string'
    })
    return datapackage

def process_row(row, row_index, resource_descriptor, resource_index, parameters, stats):
    if resource_index == 0:
        row[parameters['column-name']] = parameters['value']
    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)
```

```python
# Row counter
from datapackage_pipelines.wrapper import process

def modify_datapackage(datapackage, parameters, stats):
    stats['row-count'] = 0
    return datapackage

def process_row(row, row_index, resource_descriptor, resource_index, parameters, stats):
    stats['row-count'] += 1
    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)
```

### Low Level Processor API

In some cases, the high-level API might be too restricting. In these cases you should consider using the low-level API.

```python
from datapackage_pipelines.wrapper import ingest, spew

if __name__ == '__main__':
  with ingest() as ctx:

    # Initialisation code, if needed

    # Do stuff with datapackage
    # ...

    stats = {}

    # and resources:
    def new_resource_iterator(resource_iterator_):
        def resource_processor(resource_):
            # resource_.spec is the resource descriptor
            for row in resource_:
                # Do something with row
                # Perhaps collect some stats here as well
                yield row
        for resource in resource_iterator_:
            yield resource_processor(resource)

    spew(ctx.datapackage,
         new_resource_iterator(ctx.resource_iterator),
         ctx.stats)
```

The above code snippet shows the structure of most low-level processors.

We always start with calling `ingest()` - this method gives us the context, containing the execution parameters, the data-package descriptor (as outputed from the previous step) and an iterator on all streamed resources' rows.

We finish the processing by calling `spew()`, which sends the processed data to the next processor in the pipeline. `spew` receives:
* A modified data-package descriptor;
* A (possibly new) iterator on the resources;
* A stats object which will be added to stats from previous steps and returned to the user upon completion of the pipeline, and;
* Optionally, a `finalizer` function that will be called after it has finished iterating on the resources, but before signalling to other processors that it's finished. You could use it to close any open files, for example.

#### A more in-depth explanation

`spew` writes the data it receives in the following order:

- First, the `datapackage` parameter is written to the stream.
  This means that all modifications to the data-package descriptor must be done _before_ `spew` is called.
  One common pitfall is to modify the data-package descriptor inside the resource iterator - try to avoid that, as the descriptor that the next processor will receive will be wrong.
- Then it starts iterating on the resources. For each resource, it iterates on its rows and writes each row to the stream.
  This iteration process eventually causes an iteration on the original resource iterator (the one that's returned from `ingest`). In turn, this causes the process' input stream to be read. Because of the way buffering in operating systems work, "slow" processors will read their input slowly, causing the ones before them to sleep on IO while their more CPU intensive counterparts finish their processing. "quick" processors will not work aimlessly, but instead will either sleep while waiting for incoming data or while waiting for their output buffer to drain.
  What is achieved here is that all rows in the data are processed more or less at the same time, and that no processor works too "far ahead" on rows that might fail in subsequent processing steps.
- Then the stats are written to the stream. This means that stats can be modified during the iteration, and only the value after the iteration finishes will be used.
- Finally, the `finalizer` method is called (if we received one).

#### A few examples

We'll start with the same processors from above, now implemented with the low level API.

```python
# Add license information
from datapackage_pipelines.wrapper import ingest, spew

if __name__ == '__main__':
  with ingest() as ctx:
    ctx.datapackage['license'] = 'MIT'
    spew(ctx.datapackage, ctx.resource_iterator)
```

```python
# Add new column with constant value to first resource
# Column name and value are taken from the processor's parameters
from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, resource_iterator = ingest()

datapackage['resources'][0]['schema']['fields'].append({
   'name': parameters['column-name'],
   'type': 'string'
})

def new_resource_iterator(resource_iterator_):
    def resource_processor(resource_):
        for row in resource_:
            row[parameters['column-name']] = parameters['value']
            yield row

    first_resource = next(resource_iterator_)
    yield(resource_processor(first_resource))

    for resource in resource_iterator_:
        yield resource

spew(datapackage, new_resource_iterator(resource_iterator))
```

```python
# Row counter
from datapackage_pipelines.wrapper import ingest, spew

_, datapackage, resource_iterator = ingest()

stats = {'row-count': 0}

def new_resource_iterator(resource_iterator_):
    def resource_processor(resource_):
        for row in resource_:
            stats['row-count'] += 1
            yield row

    for resource in resource_iterator_:
        yield resource_processor(resource)

spew(datapackage, new_resource_iterator(resource_iterator), stats)
```

This next example shows how to implement a simple web scraper. Although not strictly required, web scrapers are usually the first processor in a pipeline. Therefore, they can ignore the incoming data-package and resource iterator, as there's no previous processor generating data:

```python
# Web Scraper
import requests
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

parameters, _, _ = ingest()

host = parameters['ckan-instance']
package_list_api = 'https://{host}/api/3/action/package_list'
package_show_api = 'https://{host}/api/3/action/package_show'

def scrape_ckan(host_):
    all_packages = requests.get(package_list_api.format(host=host_))\
                           .json()\
                           .get('result', [])
    for package_id in all_packages:
      params = dict(id=package_id)
      package_info = requests.get(package_show_api.format(host=host_),
                                  params=params)\
                             .json()\
                             .get('result')
      if result is not None:
        yield dict(
            package_id=package_id,
            author=package_info.get('author'),
            title=package_info.get('title'),
        )

datapackage = {
  'resources': [
    {
      PROP_STREAMING: True,   # You must set this property for resources being streamed in the pipeline!
      'name': 'package-list',
      'schema': {
        'fields': [
          {'name': 'package_id', 'type': 'string'},
          {'name': 'author',     'type': 'string'},
          {'name': 'title',      'type': 'string'},
        ]
      }
    }
  ]
}

spew(datapackage, [scrape_ckan(host)])
```

In this example we can see that the initial datapackage is generated from scratch, and the resource iterator is in fact a scraper, yielding rows as they are received from the CKAN instance API.

## Plugins and Source Descriptors

When writing pipelines in a specific problem domain, one might discover that the processing pipelines that are developed follow a certain pattern. Scraping, or fetching source data tends to be similar to one another. Processing, data cleaning, validation are often the same.

In order to ease maintenance and avoid boilerplate, a _`datapackage-pipelines` **plugin**_ can be written.

Plugins are Python modules named `datapackage_pipelines_<plugin-name>`. Plugins can provide two facilities:

- Processor packs - you can pack processors revolving a certain theme or for a specific purpose in a plugin. Any processor `foo` residing under the `datapackage_pipelines_<plugin-name>.processors` module can be used from within a pipeline as `<plugin-name>.foo`.
- Pipeline templates - if the class `Generator` exists in the `datapackage_pipelines_<plugin-name>` module, it will be used to generate pipeline based on templates - which we call "source descriptors".

### Source Descriptors

A source descriptor is a yaml file containing information which is used to create a full pipeline.

`dpp` will look for files named `<plugin-name>.source-spec.yaml` , and will treat them as input for the pipeline generating code - which should be implemented in a class called `Generator` in the `datapackage_pipelines_<plugin-name>` module.

This class should inherit from `GeneratorBase` and should implement two methods:

- `generate_pipeline` -
   which receives the source description and returns an iterator of tuples of the form `(id, details)`.
   `id` might be a pipeline id, in which case details would be an object containing the pipeline definition.
   If `id` is of the form `:module:`, then the details are treated as a source spec from the specified module. This way a generator might generate other source specs.
- `get_schema` - which should return a JSON Schema for validating the source description's structure

#### Example

Let's assume we write a `datapackage_pipelines_ckan` plugin, used to pull data out of [CKAN](https://ckan.org) instances.

Here's how such a hypothetical generator would look like:

```python
import os
import json

from datapackage_pipelines.generators import \
    GeneratorBase, slugify, steps, SCHEDULE_MONTHLY

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        pipeline_id = dataset_name = slugify(source['name'])
        host = source['ckan-instance']
        action = source['data-kind']

        if action == 'package-list':
            schedule = SCHEDULE_MONTHLY
            pipeline_steps = steps(*[
                ('ckan.scraper', {
                   'ckan-instance': host
                }),
                ('metadata', {
                  'name': dataset_name
                }),
                ('dump_to_zip', {
                   'out-file': 'ckan-datapackage.zip'
                })])
            pipeline_details = {
                'pipeline': pipeline_steps,
                'schedule': {'crontab': schedule}
            }
            yield pipeline_id, pipeline_details
```

In this case, if we store a `ckan.source-spec.yaml` file looking like this:

```yaml
ckan-instance: example.com
name: example-com-list-of-packages
data-kind: package-list
```

Then when running `dpp` we will see an available pipeline named `./example-com-list-of-packages`

This pipeline would internally be composed of 3 steps: `ckan.scraper`, `metadata` and `dump_to_zip`.

#### Validating Source Descriptors

Source descriptors can have any structure that best matches the parameter domain of the output pipelines. However, it must have a consistent structure, backed by a JSON Schema file. In our case, the Schema might look like this:

```json
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "name":          { "type": "string" },
    "ckan-instance": { "type": "string" },
    "data-kind":     { "type": "string" }
  },
  "required": [ "name", "ckan-instance", "data-kind" ]
}
```

`dpp` will ensure that source descriptor files conform to that schema before attempting to convert them into pipelines using the `Generator` class.

#### Providing Processor Code

In some cases, a generator would prefer to provide the processor code as well (alongside the pipeline definition).
In order to to that, the generator can add a `code` attribute to any step containing the processor's code. When executed, this step won't try to resolve the processor as usual but will the provided code instead.

## Running on a schedule

`datapackage-pipelines` comes with a celery integration, allowing for pipelines to be run at specific times via a `crontab` like syntax.

In order to enable that, you simply add a `schedule` section to your `pipeline-spec.yaml` file (or return a schedule from the generator class, see above), like so:

```yaml
co2-information-cdiac:
  pipeline:
    -
        ...
  schedule:
    # minute hour day_of_week day_of_month month_of_year
    crontab: '0 * * * *'
```

In this example, this pipeline is set to run every hour, on the hour.

To run the celery daemon, use `celery`'s command line interface to run `datapackage_pipelines.app`. Here's one way to do it:

```shell
$ python -m celery worker -B -A datapackage_pipelines.app
```

Running this server will start by executing all "dirty" tasks, and continue by executing tasks based on their schedules.

As a shortcut for starting the scheduler and the dashboard (see below), you can use a prebuilt _Docker_ image:

```bash
$ docker run -v `pwd`:/pipelines:rw -p 5000:5000 \
        frictionlessdata/datapackage-pipelines server
```

And then browse to `http://<docker machine's IP address>:5000/` to see the current execution status dashboard.

## Pipeline Dashboard & Status Badges

When installed on a server or running using the task scheduler, it's often very hard to know exactly what's running and what the status is of each pipeline.

To make things easier, you can spin up the web dashboard to provide an overview of each pipeline's status, its basic info and the result of it latest execution.

To start the web server run `dpp serve` from the command line and browse to http://localhost:5000

The environment variable `DPP_BASE_PATH` will determine whether dashboard will be served from root or from another base path (example value: `/pipelines/`).

The dashboard endpoints can be made to require authentication by adding a username and password with the environment variables `DPP_BASIC_AUTH_USERNAME` and `DPP_BASIC_AUTH_PASSWORD`.

Even simpler pipeline status is available with a status badge, both for individual pipelines, and for pipeline collections. For a single pipeline, add the full pipeline id to the badge endpoint:

```
http://localhost:5000/badge/path_to/pipelines/my-pipeline-id
```

![](https://img.shields.io/badge/pipeline-succeeded%20(30756%20records)-brightgreen.svg)

![](https://img.shields.io/badge/pipeline-invalid-lightgrey.svg)

![](https://img.shields.io/badge/pipeline-failed-red.svg)

![](https://img.shields.io/badge/pipeline-not%20found-lightgray.svg)

Or for a collection of pipelines:

```
http://localhost:5000/badge/collection/path_to/pipelines/
```

![](https://img.shields.io/badge/pipelines-22%20succeeded-brightgreen.svg)

![](https://img.shields.io/badge/pipelines-4%20running%2C%20%201%20succeeded%2C%205%20queued-yellow.svg)

![](https://img.shields.io/badge/pipelines-11%20succeeded%2C%207%20failed%2C%201%20invalid-red.svg)

![](https://img.shields.io/badge/pipelines-not%20found-lightgray.svg)

Note that these badge endpoints will always be exposed regardless of `DPP_BASIC_AUTH_PASSWORD` and `DPP_BASIC_AUTH_USERNAME` settings.

## Integrating with other services

Datapackage-pipelines can call a predefined webhook on any pipeline event. This might allow for potential integrations with other applications.

In order to add a webhook in a specific pipeline, add a `hooks` property in the pipeline definition, which should be a list of URLs.
Whenever that pipeline is queued, starts running or finishes running, all the urls will be POSTed with this payload:
```json
{
  "pipeline": "<pipeline-id>",
  "event": "queue/start/progress/finish",
  "success": true/false (when applicable),
  "errors": [list-of-errors, when applicable]
}
```

## Known Issues

* loading a resource which has a lot of data in a single cell raises an exception ([#112](https://github.com/frictionlessdata/datapackage-pipelines/pull/112#issue-160766294))
