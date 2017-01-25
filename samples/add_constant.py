# Add new column with constant value to first resource
# Column name and value are taken from the processor's parameters
from datapackage_pipelines.wrapper import process


def modify_datapackage(datapackage, parameters, _):
    datapackage['resources'][0]['schema']['fields'].append({
      'name': parameters['column-name'],
      'type': 'string'
    })
    return datapackage


def process_row(row, _1, _2, resource_index, parameters, _):
    if resource_index == 0:
        row[parameters['column-name']] = parameters['value']
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
