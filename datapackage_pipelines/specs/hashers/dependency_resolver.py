import datapackage
from datapackage.exceptions import DataPackageException
from ..parsers.base_parser import PipelineSpec

from ..errors import SpecError


class DependencyMissingException(Exception):

    def __init__(self, spec, missing):
        self.spec = spec
        self.missing = missing


def resolve_dependencies(spec: PipelineSpec, all_pipeline_ids, status_mgr):

    cache_hash = ''
    dependencies = spec.pipeline_details.get('dependencies', ())
    for dependency in dependencies:
        if 'pipeline' in dependency:
            pipeline_id = dependency['pipeline']
            if pipeline_id not in all_pipeline_ids:
                raise DependencyMissingException(spec, pipeline_id)

    for dependency in dependencies:
        if 'pipeline' in dependency:
            pipeline_id = dependency['pipeline']
            ps = status_mgr.get(pipeline_id)
            if not ps.runnable():
                spec.validation_errors.append(
                    SpecError('Invalid dependency',
                              'Cannot run until dependency passes validation: {}'.format(pipeline_id))
                )
            elif ps.dirty():
                spec.validation_errors.append(
                    SpecError('Dirty dependency',
                              'Cannot run until dependency is executed: {}'.format(pipeline_id))
                )
            elif ps.get_last_execution() is not None and not ps.get_last_execution().success:
                spec.validation_errors.append(
                    SpecError('Dependency unsuccessful',
                              'Cannot run until dependency "{}" is successfully '
                              'executed'.format(pipeline_id))
                )

            for dep_err in ps.validation_errors:
                spec.validation_errors.append(
                    SpecError('From {}'.format(pipeline_id), dep_err)
                )

            pipeline_hash = all_pipeline_ids.get(pipeline_id).cache_hash
            assert pipeline_hash is not None
            cache_hash += pipeline_hash

            spec.dependencies.append(pipeline_id)

        elif 'datapackage' in dependency:
            dp_id = dependency['datapackage']
            try:
                dp = datapackage.DataPackage(dp_id)
                if 'hash' in dp.descriptor:
                    cache_hash += dp.descriptor['hash']
                else:
                    spec.validation_errors.append(
                        SpecError('Missing dependency',
                                  "Couldn't get data from datapackage %s"
                                  % dp_id))
            except DataPackageException:
                spec.validation_errors.append(
                    SpecError('Missing dependency',
                              "Couldn't open datapackage %s"
                              % dp_id))

        else:
            spec.validation_errors.append(
                SpecError('Missing dependency',
                          'Unknown dependency provided (%r)' % dependency))

    return cache_hash
