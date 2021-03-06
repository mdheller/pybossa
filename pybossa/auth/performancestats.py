# -*- coding: utf8 -*-
# This file is part of PYBOSSA.
#
# Copyright (C) 2015 Scifabric LTD.
#
# PYBOSSA is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PYBOSSA is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with PYBOSSA.  If not, see <http://www.gnu.org/licenses/>.


class PerformanceStatsAuth(object):
    _specific_actions = []

    def __init__(self, project_repo):
        self.project_repo = project_repo

    @property
    def specific_actions(self):
        return self._specific_actions

    def can(self, user, action, stat=None):
        action = ''.join(['_', action])
        return getattr(self, action)(user, stat)

    def _read(self, user, stat=None):
        if not user.is_authenticated:
            return False
        if not stat:
            return True
        if user.admin or user.subadmin:
            return True
        project = self.project_repo.get(stat.project_id)
        return user.id in project.owners_ids
