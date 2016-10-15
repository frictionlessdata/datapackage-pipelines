# datapackage-pipelines

A modular framework for a stream-processing ETL based on Data Packages

[![Travis](https://img.shields.io/travis/frictionlessdata/datapackage-pipelines/master.svg)](https://travis-ci.org/frictionlessdata/datapackage-pipelines)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/datapackage-pipelines.svg?branch=master)](https://coveralls.io/r/frictionlessdata/datapackage-pipelines?branch=master)

## QUICK START

```
# Install from PyPi
$ pip install datapackage-pipelines

# The pipeline definition
$ cat > pipeline-spec.yaml
albanian-treasury:
  schedule:
    crontab: '0 * * * *'
  pipeline:
    -
      run: simple_remote_source
      parameters:
        resources:
          -
            url: "https://raw.githubusercontent.com/openspending/fiscal-data-package-demos/master/al-treasury-spending/data/treasury.csv"
            schema:
              fields:
                -
                  name: "Date executed"
                  type: date
                  osType: date:generic
                -
                  name: "Value"
                  type: number
                  osType: value
                -
                  name: "Supplier"
                  type: string
                  osType: supplier:generic:name
    -
      run: model
    -
      run: metadata
      parameters:
        metadata:
          name: 'al-treasury-spending'
          title: 'Albania Treasury Service'
          granularity: transactional
          countryCode: AL
          homepage: 'http://spending.data.al/en/treasuryservice/list/year/2014/inst_code/1005001'

    -
      run: downloader
    -
      run: dump
      parameters:
          out-file: al-treasury-spending.zip
^D

# List Available Pipelines
$ dpp
Available Pipelines:
- ./albanian-treasury

# Invoke the pipeline manually
$ dpp ./albanian-treasury
INFO    :Main                            :RUNNING ./albanian-treasury:
INFO    :Main                            :- /Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/simple_remote_source.py
INFO    :Main                            :- /Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/model.py
INFO    :Main                            :- /Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/metadata.py
INFO    :Main                            :- /Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/downloader.py
INFO    :Main                            :- /Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/dump.py
INFO    :Simple_Remote_Source            :Processed 0 rows
INFO    :Model                           :Processed 0 rows
INFO    :Metadata                        :Processed 0 rows
INFO    :Downloader                      :Starting new HTTPS connection (1): raw.githubusercontent.com
DEBUG   :Downloader                      :"GET /openspending/fiscal-data-package-demos/master/al-treasury-spending/data/treasury.csv HTTP/1.1" 200 3784
INFO    :Downloader                      :TOTAL 40 rows
INFO    :Downloader                      :Processed 40 rows
INFO    :Dump                            :Processed 40 rows
INFO    :Main                            :WAITING FOR ./albanian-treasury:/Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/simple_remote_source.py
INFO    :Main                            :WAITING FOR ./albanian-treasury:/Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/model.py
INFO    :Main                            :WAITING FOR ./albanian-treasury:/Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/metadata.py
INFO    :Main                            :WAITING FOR ./albanian-treasury:/Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/downloader.py
INFO    :Main                            :WAITING FOR ./albanian-treasury:/Users/adam/code/os/datapackage-pipelines/datapackage_pipelines/manager/../lib/dump.py
INFO    :Main                            :DONE ./albanian-treasury: [0, 0, 0, 0, 0]

# Examine Results
$ unzip -t al-treasury-spending.zip
Archive:  al-treasury-spending.zip
    testing: datapackage.json         OK
    testing: data/treasury.csv        OK
No errors detected in compressed data of al-treasury-spending.zip.

$ unzip -p al-treasury-spending.zip datapackage.json | json_pp
{
   "name" : "al-treasury-spending",
   "granularity" : "transactional",
   "homepage" : "http://spending.data.al/en/treasuryservice/list/year/2014/inst_code/1005001",
   "countryCode" : "AL",
   "resources" : [
      {
         "schema" : {
            "fields" : [
               {
                  "slug" : "Date_executed",
                  "title" : "Date executed",
                  "type" : "date",
                  "format" : "fmt:%Y-%m-%d",
                  "osType" : "date:generic",
                  "conceptType" : "date",
                  "name" : "Date executed"
               },
               {
                  "type" : "number",
                  "decimalChar" : ".",
                  "slug" : "Value",
                  "conceptType" : "value",
                  "format" : "default",
                  "osType" : "value",
                  "name" : "Value",
                  "title" : "Value",
                  "groupChar" : ","
               },
               {
                  "title" : "Supplier",
                  "slug" : "Supplier",
                  "name" : "Supplier",
                  "format" : "default",
                  "osType" : "supplier:generic:name",
                  "type" : "string",
                  "conceptType" : "supplier"
               }
            ],
            "primaryKey" : [
               "Date executed"
            ]
         },
         "path" : "data/treasury.csv"
      }
   ],
   "title" : "Albania Treasury Service",
   "model" : {
      "measures" : {
         "Value" : {
            "source" : "Value",
            "title" : "Value"
         }
      },
      "dimensions" : {
         "supplier" : {
            "attributes" : {
               "Supplier" : {
                  "title" : "Supplier",
                  "source" : "Supplier"
               }
            },
            "primaryKey" : [
               "Supplier"
            ],
            "dimensionType" : "entity"
         },
         "date" : {
            "dimensionType" : "datetime",
            "primaryKey" : [
               "Date_executed"
            ],
            "attributes" : {
               "Date_executed" : {
                  "title" : "Date executed",
                  "source" : "Date executed"
               }
            }
         }
      }
   }
}
```

## Documentation

This framework can run pipelines of data processing modules which are basically a list of steps which implement an ETL
  process. Each step is an *executor*, which can be one of these: 

 - Extractor: Python scripts which get tabular data located somewhere on the web and output a Fiscal Data Package (FDP).
 - Transformer: Python scripts which get an FDP as input, modify it and output a new FDP.
 - Loader: Python scripts which get an FDP as input, upload it to some web service or storage and output it once more.

Each pipeline also has a set of rules that define a schedule and a pipeline of such executors, working serially to 
    fetch data from the Internet, process it into a finalized FDP and load it to an external destination.

All processing in this framework is done by processing the streams of data, row by row. At no point the entire data-set
is loaded into memory. This allows efficient processing in terms of memory usage as well as truly parallel execution
of all processing steps, making use of your machine's CPU effectively.

## Running Instructions

Running instructions are stored in files named `pipeline-spec.yaml`. 

Each one of these files is a YAML file which contains instructions for fetching one or more FDPs. For example, such a 
file might look like this:

```
albonian-spending:
    schedule:
        cron: '3 0 * * *'
    pipeline:
        - 
            run: fetch-albonian-fiscal-data
            parameters:
                kind: 'expenditures'
        -   
            run: translate-codelists
        -
            run: normalize-dates
albonian-budget:
    schedule:
        cron: '0 0 7 1 *'
    pipeline:
        - 
            run: fetch-albonian-fiscal-data
            parameters:
                kind: 'budget'
        -   
            run: translate-codelists
```

**What do we have here?**

Two running instructions for two separate data packages - one fetching the Albonian spending data and another fetching 
its budget data. You can see that the pipelines are very similar, and are based on the same building blocks: 
 `fetch-albonian-fiscal-data`, `translate-codelists` and `normalize-dates`. The differences between the two are 
 - their schedules: spending data is fetched on a daily basis, whilst budgets are fetched on January 7th every year 
        (Albonian government officials adhere to very precise publishing dates)
 - the running parameters for the `fetch-albonian-fiscal-data` executor are different - 
 so that code is reused and controlled via running parameters
 - the pipeline for spending data has an extra step (`normalize-dates`)
 
**Spec:**

This YAML file is basically a mapping between *Pipeline IDs* to their specs. Task IDs are the way we reference the
pipeline in various places so choose wisely.

A pipeline spec has two keys:
 - `schedule`: can have one sub-key, which can currently be only `crontab`. The value for the former is a standard
    `crontab` schedule row.
 - `pipeline`: a list of steps, each is an object with the following properties:
    - `run`: the name of the executor - a Python script which will perform the step's actions.
        This script is searched in the current directory (read: where the running instructions file is located), 
        in paths specified in the `DATAPIPELINES_PROCESSOR_PATH` environment variable, or in the common lib 
        of executors (in that order).
        Relative paths can be specified with the 'dot-notation': `a.b` is referring to script `b` in directory `a`; 
        `...c.d.e` will look for `../../c/d/e.py`. 
    - `parameters`: running parameters which the executor will receive when invoked.
    - `validate`: should data be validated prior to entering this executor. Data validation is done using the JSON table
        schema which is embedded in the resource definition.
     
The first executor in all pipelines must be a fetcher and the rest of the steps must be processors.
 
## Executors

Executors are Python scripts with a simple API, based on their standard input & standard output streams (as well as
  command line parameters).

All executors output an FDP to the standard output. This is done in the following way:
 - The first line printed to `stdout` must be the contents of the `datapackage.json` - that is, a JSON object without
  any newlines.
 - After that first line, tabular data files can be appended (we don't support any other kind of files ATM).
   Each tabular data file must be printed out in the following way:
     - First line must always be an empty line (that is, just a single newline character).
     - Subsequent lines contain the contents of the data rows of the file (i.e. no header row or other chaff)
     - Each row in the file must be printed as a single-line JSON encoded object, which maps the header names to values
     
Processors will receive an FDP in the exact same format in their stdin. Fetchers will receive nothing in their stdin.

Parameters are passed as a JSON encoded string in the first command line argument of the executor.

Files should appear in the same order as the resources defined in the FDP. Only data for local files is expected - 
 remote resources can just be ignored.
      
### Why JSON and not CSV?

Well, for a multitude of reasons:
 - JSON encoding is not dependent on locale settings of the executing machine
 - JSON has better type indication: strings vs. numbers vs. booleans vs. missing values (with time and date values as 
  the only exception)
 - JSON is easier to work with in Python
 
*What about time and dates, then?* 
Just use their string representation and make sure that the JSON Table Schema contains the correct format definition
 for that field.
 
The framework will take these JSONs and convert them to proper CSV files before uploading - with a correct dialect, 
encoding and locale info.

## Developing Executors

To avoid boilerplate, the `ingest` and `spew` utility functions for executors can come in handy:

```python

from executor_util import ingest, spew

if __name__=="__main__":
  params, fdp, resource_iterator = ingest()
  
  # do something with fdp
  # ...
  
  def resource_processor(row_iterator):
    resource_spec = row_iterator.spec
    # you can modify the resource if needed here
    for row in row_iterator:
      # do something with row
      # ...
      yield row
      
  spew(fdp, (process_resource(r) for r in resource_iterator))
  
```
  
## Running the Datapackage-Pipeline Deamon

```
$ python -m celery worker -B -A datapackage_pipelines.app
```

Will run all pipelines based on their defined schedule using `celery`.


## Contributing

Please read the contribution guideline:

[How to Contribute](CONTRIBUTING.md)

Thanks!
