import os
import zipfile
import tempfile

from datapackage_pipelines.wrapper import ingest, spew

import gobble

params, datapackage, res_iter = ingest()
spew(datapackage, res_iter)

user = gobble.user.User()
in_filename = open(params['in-file'], 'rb')

in_file = zipfile.ZipFile(in_filename)
temp_dir = tempfile.mkdtemp()
for name in in_file.namelist():
    in_file.extract(name, temp_dir)
in_file.close()
datapackage_json = os.path.join(temp_dir, 'datapackage.json')

package = gobble.fiscal.FiscalDataPackage(datapackage_json, user=user)
package.upload(skip_validation=True)
