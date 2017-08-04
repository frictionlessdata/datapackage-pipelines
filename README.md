# Datapackage Pipelines

[![Travis](https://img.shields.io/travis/frictionlessdata/datapackage-pipelines/master.svg)](https://travis-ci.org/frictionlessdata/datapackage-pipelines) [![Coveralls](http://img.shields.io/coveralls/frictionlessdata/datapackage-pipelines.svg?branch=master)](https://coveralls.io/r/frictionlessdata/datapackage-pipelines?branch=master)

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
      run: add_metadata
      parameters:
        name: 'co2-emissions'
        title: 'CO2 emissions (metric tons per capita)'
        homepage: 'http://worldbank.org/'
    -
      run: add_resource
      parameters:
        name: 'global-data'
        url: "http://api.worldbank.org/v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel"
        format: xls
        headers: 4
    -
      run: stream_remote_resources
      cache: True
    -
      run: set_types
      parameters:
         resources: global-data
         types:
           "[12][0-9]{3}":
              type: number
    -
      run: dump.to_zip
      parameters:
          out-file: co2-emissions-wb.zip     
```

In this example we see one pipeline called `worldbank-co2-emissions`. Its pipeline consists of 4 steps:

- `metadata`: This is a library processor  (see below), which modifies the data-package's descriptor (in our case: the initial, empty descriptor) - adding `name`, `title` and other properties to the datapackage.
- `add_resource`: This is another library processor, which adds a single resource to the data-package.
  This resource has a `name` and a `url`, pointing to the remote location of the data.
- `stream_remote_resources`: This processor will convert remote resources (like the one we defined in the 1st step) to local resources, streaming the data to processors further down the pipeline (see more about streaming below).
- `set_types`: This processor assigns data types to fields in the data. In this example, field headers looking like years will be assigned the `number` type.
- `dump.to_zip`: Create a zipped and validated datapackage with the provided file name.

### Mechanics 

An important aspect of how the pipelines are run is the fact that data is passed in streams from one processor to another. If we get "technical" here, then each processor is run in its own dedicated process, where the datapackage is read from its `stdin` and output to its `stdout`. The important thing to note here is that no processor holds the entire data set at any point. 

This limitation is by design - to keep the memory and disk requirements of each processor limited and independent of the dataset size.

### Quick Start

First off, create a `pipeline-spec.yaml` file in your current directory. You can take the above file if you just want to try it out.

Then, you can either install `datapackage-pipelines` locally:

```shell
$ pip install datapackage-pipelines

$ dpp
Available Pipelines:
- ./worldbank-co2-emissions (*)

$ dpp run ./worldbank-co2-emissions
INFO :Main:RUNNING ./worldbank-co2-emissions
INFO :Main:- lib/add_metadata.py
INFO :Main:- lib/add_resource.py
INFO :Main:- lib/stream_remote_resources.py
INFO :Main:- lib/dump/to_zip.py
INFO :Main:DONE lib/add_metadata.py
INFO :Main:DONE lib/add_resource.py
INFO :Main:stream_remote_resources: OPENING http://api.worldbank.org/v2/en/indicator/EN.ATM.CO2E.PC?downloadformat=excel
INFO :Main:stream_remote_resources: TOTAL 264 rows
INFO :Main:stream_remote_resources: Processed 264 rows
INFO :Main:DONE lib/stream_remote_resources.py
INFO :Main:dump.to_zip: INFO :Main:Processed 264 rows
INFO :Main:DONE lib/dump/to_zip.py
INFO :Main:RESULTS:
INFO :Main:SUCCESS: ./worldbank-co2-emissions 
                    {'dataset-name': 'co2-emissions', 'total_row_count': 264}
```

(Requirements: _Python 3.6_ or higher)

Alternatively, you could use our docker image:

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
      pipeline: ./geo/regoin-areal
    - 
      datapackage: http://pets.net/data/dogs-per-regoin/datapackage.json
    - 
      datapackage: http://pets.net/data/dogs-per-regoin
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

## The Standard Processor Library

A few built in processors are provided with the library.

### ***`add_metadata`***

Adds meta-data to the data-package.

_Parameters_:

Any allowed property (according to the [spec]([http://specs.frictionlessdata.io/data-packages/#metadata)) can be provided here.

*Example*:

```yaml
- run: add_metadata
  parameters: 
    name: routes-to-mordor
    license: CC-BY-SA-4
    author: Frodo Baggins <frodo@shire.me>
    contributors:
      - samwise gamgee <samwise1992@yahoo.com>
```

### ***`add_resource`***

Adds a new remote tabular resource to the data-package. 

_Parameters_:

You should provide the `name` and `url` attributes, and other optional attributes as defined in the [spec]([http://specs.frictionlessdata.io/data-packages/#resource-information).

Note that `url` also supports `env://<environment-variable>`, which indicates that the resource url should be fetched from the indicated environment variable.  This is useful in case you are supplying a string with sensitive information (such as an SQL connection string for streaming from a database table).

Parameters are basically arguments that are passed to a `tabulator.Stream` instance (see the [API](https://github.com/frictionlessdata/tabulator-py#api-reference)).
Other than those, you can pass a `constants` parameter which should be a mapping of headers to string values.
When used in conjunction with `stream_remote_resources`, these constant values will be added to each generated row 
(as well as to the default schema). 

You may also provide a schema here, or use the default schema generated by the `stream_remote_resources` processor.
In case `path` is specified, it will be used. If not, the `stream_remote_resources` processor will assign the `path` for you with a `csv` extension.

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

Converts remote resources to streamed resources.

Remote resources are ones that link to a remote data source, but are not processed by the pipeline and are kept as-is.

Streamed resources are ones that can be processed by the pipeline, and their output is saved as part of the resulting datapackage.

In case a resource has no schema, a default one is generated automatically here by creating a `string` field from each column in the data source.

_Parameters_:

- `resources` - Which resources to stream. Can be:

  - List of strings, interpreted as resource names to stream
  - String, interpreted as a regular expression to be used to match resource names

  If omitted, all resources in datapackage are streamed.

- `ignore-missing` - if true, then missing resources won't raise an error but will be treated as 'empty' (i.e. with zero rows). 
  Resources with empty URLs will be treated the same (i.e. will generate an 'empty' resource).

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

### ***`set_types`***

Sets data types and type options to fields in streamed resources, and make sure that the data still validates with the new types. 

This allows to make modification to the existing table schema, and usually to the default schema from `stream_remote_resources`.

_Parameters_:

-  `resources` - Which resources to modify. Can be:

   - List of strings, interpreted as resource names to stream
   - String, interpreted as a regular expression to be used to match resource names

   If omitted, all resources in datapackage are streamed.

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
      "yealry_score_[0-9]{4}": 
        type: number
      "date of birth":
        type: date
        format: "fmt:dd/mm/YYYY"
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

`resource` can be 
   - List of strings, interpreted as resource names to load
   - String, interpreted as a regular expression to be used to match resource names
   - an integer, indicating the index of the resource in the data package (0-based) 

All properties of the loaded resource will be copied - `path` and `schema` included.

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
  
Both `in` and `out` should be a list of objects.

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

### ***`dump.to_sql`***

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

### ***`dump.to_path`***

Saves the datapackage to a filesystem path.

_Parameters_:

- `out-path` - Name of the output path where `datapackage.json` will be stored.

  This path will be created if it doesn't exist, as well as internal data-package paths.

  If omitted, then `.` (the current directory) will be assumed.

- `force-format` - Specifies whether to force all output files to be generated with the same format
    - if `true` (the default), all resources will use the same format
    - if `false`, format will be deduced from the file extension. Resources with unknown extenstions will be discrarded.
- `format` - Specifies the type of output files to be generated (if `force-format` is true): `csv` (the default) or `json`
- `handle-non-tabular` - Specifies whether non tabular resources (i.e. resources without a `schema`) should be dumped as well to the resulting datapackage.
    (See note below for more details)
- `counters` - Specifies whether to count rows, bytes or md5 hash of the data and where it should be stored. An object with the following properties:
    - `datapackage-rowcount`: Where should a total row count of the datapackage be stored (default: `count_of_rows`)
    - `datapackage-bytes`: Where should a total byte count of the datapackage be stored (default: `bytes`)
    - `datapackage-hash`: Where should an md5 hash of the datapackage be stored (default: `hash`)
    - `resource-rowcount`: Where should a total row count of each resource be stored (default: `count_of_rows`)
    - `resource-bytes`: Where should a total byte count of each resource be stored (default: `bytes`)
    - `resource-hash`: Where should an md5 hash of each resource be stored (default: `hash`)
    Each of these attributes could be set to null in order to prevent the counting.
    Each property could be a dot-separated string, for storing the data inside a nested object (e.g. `stats.rowcount`)

### ***`dump.to_zip`***

Saves the datapackage to a zipped archive.

_Parameters_:

- `out-file` - Name of the output file where the zipped data will be stored
- `force-format` and `format` - Same as in `dump.to_path` 
- `handle-non-tabular` - Same as in `dump.to_path` 
- `counters` - Same as in `dump.to_path` 

#### *Note*

`dump.to_path` and `dump.to_zip` processors will handle non-tabular resources as well.
These resources must have both a `url` and `path` properties, and _must not_ contain a `schema` property.
In such cases, the file will be downloaded from the `url` and placed in the provided `path`.

## Custom Processors

It's quite reasonable that for any non-trivial processing task, you might encounter a problem that cannot be solved using the standard library processors.

For that you might need to write your own processor - here's how it's done.

There are two APIs for writing processors - the high level API and the low level API.

**Important**: due to the way that pipeline execution is implemented, you **cannot** `print` from within a processor. In case you need to debug, _only_ use the `logging` module to print out anything you need.

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

parameters, datapackage, resource_iterator = ingest()

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
        
spew(datapackage, new_resource_iterator(resource_iterator), stats)
```

The above code snippet shows the structure of most low-level processors.

We always start with calling `ingest()` - this method gives us the execution parameters, the data-package descriptor (as outputed from the previous step) and an iterator on all streamed resources' rows.

We finish the processing by calling `spew()`, which sends the processed data to the next processor in the pipeline. `spew` receives a modified data-package descriptor, a (possibly new) iterator on the resources and a stats object which will be added to stats from previous steps and returned to the user upon completion of the pipeline.

#### A more in-depth explanation

`spew` writes the data it receives in the following order:

- First, the `datapackage` parameter is written to the stream. 
  This means that all modifications to the data-package descriptor must be done _before_ `spew` is called.
  One common pitfall is to modify the data-package descriptor inside the resource iterator - try to avoid that, as the descriptor that the next processor will receive will be wrong.
- Then it starts iterating on the resources. For each resource, it iterates on its rows and writes each row to the stream.
  This iteration process eventually causes an iteration on the original resource iterator (the one that's returned from `ingest`). In turn, this causes the process' input stream to be read. Because of the way buffering in operating systems work, "slow" processors will read their input slowly, causing the ones before them to sleep on IO while their more CPU intensive counterparts finish their processing. "quick" processors will not work aimlessly, but instead will either sleep while waiting for incoming data or while waiting for their output buffer to drain. 
  What is achieved here is that all rows in the data are processed more or less at the same time, and that no processor works too "far ahead" on rows that might fail in subsequent processing steps.
- Finally, the stats are written to the stream. This means that stats can be modified during the iteration, and only the value after the iteration finishes will be used.

#### A few examples

We'll start with the same processors from above, now implemented with the low level API.

```python
# Add license information
from datapackage_pipelines.wrapper import ingest, spew

_, datapackage, resource_iterator = ingest()
datapackage['license'] = 'MIT'
spew(datapackage, resource_iterator)
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

In order to ease maintenance and avoid boilerplate, a _`datapackage-pipelines` **plugin**_. 

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
   If `id` is of the form `:module:`, then the details are treated as a source spec from the specified module. This way a generator a generator might generate other source specs. 
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
                ('dump.to_zip', {
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

This pipeline would internally be composed of 3 steps: `ckan.scraper`, `metadata` and `dump.to_zip`.

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

## Pipeline Dashboard

When installed on a server or running using the task scheduler, it's often very hard to know exactly what's running and what's the status of each pipeline.

To make things easier, you can spin up the web dashboard which provides an overview of each pipeline's status, its basic info and the result of it latest execution.

To start the web server run `dpp serve` from the command line and browse to http://localhost:5000

The environment variable `DPP_BASE_PATH` will determine whether dashboard will be served from root or from another base path (example value: `/pipelines/`).
