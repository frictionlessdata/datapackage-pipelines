import datapackage
from datapackage.exceptions import DataPackageException

from ..errors import SpecError
from ...status import status


class DependencyMissingException(Exception):
    pass


def resolve_dependencies(spec, all_pipeline_ids):

    cache_hash = ''
    dependencies = spec.pipeline_details.get('dependencies', ())
    for dependency in dependencies:
        if 'pipeline' in dependency:
            pipeline_id = dependency['pipeline']
            if pipeline_id not in all_pipeline_ids:
                raise DependencyMissingException()

            dirty = all_pipeline_ids.get(pipeline_id).dirty
            if dirty is True:
                spec.errors.append(
                    SpecError('Dirty dependency',
                              'Cannot run until all dependencies are executed')
                )
            if not status.is_successful(pipeline_id):
                spec.errors.append(
                    SpecError('Dependency unsuccessful',
                              'Cannot run until all dependencies are '
                              'successfully executed')
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
                    spec.errors.append(
                        SpecError('Missing dependency',
                                  "Couldn't get data from datapackage %s"
                                  % dp_id))
            except DataPackageException:
                spec.errors.append(
                    SpecError('Missing dependency',
                              "Couldn't open datapackage %s"
                              % dp_id))

        else:
            spec.errors.append(
                SpecError('Missing dependency',
                          'Unknown dependency provided (%r)' % dependency))

    return cache_hash
