# -*- coding: utf8 -*-
# This file is part of PyBossa.
#
# Copyright (C) 2017 SciFabric LTD.
#
# PyBossa is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PyBossa is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with PyBossa.  If not, see <http://www.gnu.org/licenses/>.
# Cache global variables for timeouts
import os
import tempfile
import pandas as pd
from pybossa.exporter.csv_export import CsvExporter
from pybossa.core import project_repo, uploader
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from pybossa.cache.projects import get_project_report_projectdata
from pybossa.cache.users import get_project_report_userdata
from pybossa.uploader import local
from flask import url_for, safe_join, redirect, current_app


class ProjectReportCsvExporter(CsvExporter):
    """Project reports exporter in CSV format"""

    def download_name(self, project, ty):
        """Get the filename (without) path of the file which should be downloaded.
           This function does not check if this filename actually exists!"""

        name = self._project_name_latin_encoded(project)
        filename = '%s_%s_%s_report.zip' % (str(project.id), name, ty)  # Example: 123_feynman_project_report_csv.zip
        filename = secure_filename(filename)
        return filename

    def _respond_csv(self, ty, id, info_only=False):
        empty_row = []
        p = project_repo.get(id)
        if p is not None:
            project_section = ['Project Statistics']
            project_header = ['Id', 'Name', 'Short Name', 'Total Tasks',
                              'First Task Submission', 'Last Task Submission',
                              'Average Time Spend Per Task', 'Task Redundancy']
            project_data = get_project_report_projectdata(id)
            project_csv = pd.DataFrame([project_data],
                            columns=project_header).to_csv(index=False)

            user_section = ['User Statistics']
            user_header = ['Id', 'Name', 'Fullname', 'Email', 'Admin', 'Subadmin', 'Enabled', 'Languages',
                           'Locations', 'Start Time', 'End Time', 'Timezone', 'Type of User',
                           'Additional Comments', 'Total Tasks Completed', 'Percent Tasks Completed',
                           'First Task Submission', 'Last Task Submission', 'Average Time Per Task']

            users_project_data = get_project_report_userdata(id)
            users_csv = 'No user data\n'
            if users_project_data:
                users_csv = pd.DataFrame([users_project_data],
                    columns=user_header).to_csv(index=False)
            csv_txt = 'Project Statistics\n{}\n{}'.format(project_csv, users_csv)
            return csv_txt

    def _make_zip(self, project, ty):
        name = self._project_name_latin_encoded(project)
        csv_task_generator = self._respond_csv(ty, project.id)
        if csv_task_generator is not None:
            with tempfile.NamedTemporaryFile() as datafile, \
                 tempfile.NamedTemporaryFile(delete=False) as zipped_datafile:
                try:
                    datafile.write(csv_task_generator.encode('utf-8'))
                    datafile.flush()
                    _zip = self._zip_factory(zipped_datafile.name)
                    _zip.write(
                        datafile.name,
                        secure_filename('%s_%s.csv' % (name, ty)))
                    _zip.close()
                    return dict(filepath=zipped_datafile.name,
                                filename=self.download_name(project, ty),
                                delete=True)
                except Exception:
                    if os.path.exists(zipped_datafile.name):
                        os.remove(zipped_datafile.name)
                    raise
