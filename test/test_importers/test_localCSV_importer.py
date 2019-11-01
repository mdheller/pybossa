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

from mock import patch, Mock, mock_open
from pybossa.importers.csv import BulkTaskLocalCSVImport, BulkTaskGDImport
from pybossa.encryption import AESWithGCM
from nose.tools import assert_raises
from pybossa.importers import BulkImportException
from default import with_context, Test
from nose.tools import assert_equal

def merge_dicts(dict1, dict2):
    result = dict1.copy()
    result.update(dict2)
    return result


class TestBulkTaskLocalCSVImport(Test):

    def setUp(self):
        form_data = {'type': 'localCSV', 'csv_filename': 'fakefile.csv'}
        self.importer = BulkTaskLocalCSVImport(**form_data)
        super(TestBulkTaskLocalCSVImport, self).setUp()

    def test_importer_type_local_csv(self):
        assert isinstance(self.importer, BulkTaskLocalCSVImport) is True
        # confirm object is not of type other than BulkTaskLocalCSVImport
        assert isinstance(self.importer, BulkTaskGDImport) is False

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_count_tasks_returns_0_row(self, s3_get):
        with patch('pybossa.importers.csv.io.open', mock_open(read_data='Foo,Bar\n'), create=True):
            number_of_tasks = self.importer.count_tasks()
            assert number_of_tasks is 0, number_of_tasks

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_count_tasks_returns_1_row(self, s3_get):
        with patch('pybossa.importers.csv.io.open', mock_open(read_data='Foo,Bar\n1,2\n'), create=True):
            number_of_tasks = self.importer.count_tasks()
            assert number_of_tasks is 1, number_of_tasks

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_count_tasks_returns_2_rows(self, s3_get):
        with patch('pybossa.importers.csv.io.open', mock_open(read_data='Foo,Bar\n1,2\naaa,bbb\n'), create=True):
            number_of_tasks = self.importer.count_tasks()
            assert number_of_tasks is 2, number_of_tasks

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_gold_answers_import(self, s3_get):
        expected_t1_gold_ans = {'ans': '3', 'ans2': '4', 'ans3': '5'}
        expected_t2_gold_ans = {'ans': 'a1', 'ans2': 'a2', 'ans3': 'a3'}
        with patch('pybossa.importers.csv.io.open', mock_open(read_data='Foo,Bar,ans_gold,ans2_gold,ans3_gold\n1,2,3,4,5\naaa,bbb,a1,a2,a3\n'), create=True):
            [t1, t2] = self.importer.tasks()
            assert_equal(t1['gold_answers'], expected_t1_gold_ans), t1
            assert_equal(t2['gold_answers'], expected_t2_gold_ans), t2

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    @patch('pybossa.importers.csv.data_access_levels')
    def test_priv_fields_import(self, mock_data_access, s3_get):
        mock_data_access = True
        expected_t1_priv_field = {'Bar2': '4', 'Bar': '3'}
        expected_t1_gold_ans = {'ans2': '5', 'ans': '2', 'ans3': '6'}
        expected_t2_priv_field = {'Bar2': 'd', 'Bar': 'c'}
        expected_t2_gold_ans = {'ans2': 'e', 'ans': 'b', 'ans3': 'f'}

        with patch('pybossa.importers.csv.io.open', mock_open(
            read_data='Foo,ans_gold,Bar_priv,Bar2_priv,ans2_gold,ans3_priv_gold\n1,2,3,4,5,6\na,b,c,d,e,f\n'), create=True):
            [t1, t2] = self.importer.tasks()
            assert_equal(t1['private_fields'], expected_t1_priv_field), t1
            assert_equal(t1['gold_answers'], expected_t1_gold_ans), t1
            assert_equal(t2['private_fields'], expected_t2_priv_field), t2
            assert_equal(t2['gold_answers'], expected_t2_gold_ans), t2

    @with_context
    @patch('pybossa.cloud_store_api.s3.get_s3_bucket_key')
    def test_count_tasks_encrypted(self, s3_get):
        k = Mock()
        s3_get.return_value = '', k
        cont = 'req\n1'
        cipher = AESWithGCM('abcd')
        k.get_contents_as_string.return_value = cipher.encrypt(cont)
        config = {
            'S3_IMPORT_BUCKET': 'aadf',
            'FILE_ENCRYPTION_KEY': 'abcd',
            'ENABLE_ENCRYPTION': True
        }
        with patch.dict(self.flask_app.config, config):
            number_of_tasks = self.importer.count_tasks()
            assert number_of_tasks is 1, number_of_tasks

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_typed_fields_import(self, s3_get):
        expected_t1_priv_field = {'Bar2': '4', 'Bar': '3', 'ans12': [], 'ans13': 1.3, 'ans14': True, 'ans15': None}
        expected_t1_gold_ans = {'ans2': '5', 'ans': '2', 'ans3': '6', 'ans8': False, 'ans9': -2, 'ans10': True, 'ans11': None, 'ans16': [], 'ans17': 1.3, 'ans18': True, 'ans19': None}
        expected_t1_field = {'Foo': '1', 'ans4': {'a':1} ,'ans5': 1.5, 'ans6': True, 'ans7': None}
        expected_t2_priv_field = {'Bar2': 'd', 'Bar': 'c', 'ans12': None, 'ans13': 0, 'ans14': True, 'ans15': None}
        expected_t2_gold_ans = {'ans2': 'e', 'ans': 'b', 'ans3': 'f', 'ans8': None, 'ans9': 0, 'ans10': True, 'ans11': None, 'ans16': None, 'ans17': 0, 'ans18': True, 'ans19': None}
        expected_t2_field = {'Foo': 'a', 'ans4': [1,2] ,'ans5': 3, 'ans6': False, 'ans7': None}
        fields = {
            'Foo': ['1', 'a', '7', 'g', '14', 'm'],
            'ans_gold': ['2', 'b', '8', 'h', '15', 'n'],
            'Bar_priv': ['3', 'c', '9', 'i', '16', 'o'],
            'Bar2_priv': ['4', 'd', '10', 'j', '17', 'p'],
            'ans2_gold': ['5', 'e', '11', 'k', '18', 'q'],
            'ans3_priv_gold': ['6', 'f', '12', 'l', '19', 'r'],
            'ans4_json': ['"{""a"":1}"', '"[1,2]"', '13', 'true', 'null', '"""a string in JSON"""'],
            'ans5_number': ['1.5', '3', '7', '8', '9', '10'],
            'ans6_bool': ['true', 'false', 'true', 'false', 'true', 'false'],
            'ans7_null': ['null', 'null', 'null', 'null', 'null', 'null'],
            'ans8_gold_json': ['false', 'null', '"[null, true, 1]"', '"""x"""', '8', '{}'],
            'ans9_gold_number': ['-2', '0', '3.5', '1.77777777777777777', 'NaN', '7'],
            'ans10_gold_bool': ['true', 'true', 'true', 'true', 'true', 'true'],
            'ans11_gold_null': ['null', 'null', 'null', 'null', 'null', 'null'],
            'ans12_priv_json': ['[]', 'null', 'true', '1', '2', '3'],
            'ans13_priv_number': ['1.3', '0', '1', '2', '3', '4'],
            'ans14_priv_bool': ['true', 'true', 'true', 'true', 'true', 'true'],
            'ans15_priv_null': ['null', 'null', 'null', 'null', 'null', 'null'],
            'ans16_priv_gold_json': ['[]', 'null', 'true', '1', '2', '3'],
            'ans17_priv_gold_number': ['1.3', '0', '1', '2', '3', '4'],
            'ans18_priv_gold_bool': ['true', 'true', 'true', 'true', 'true', 'true'],
            'ans19_priv_gold_null': ['null', 'null', 'null', 'null', 'null', 'null']
        }
        rows = []
        rows.append(','.join(fields.keys()))
        for i in range(6):
            rows.append(','.join(map(lambda x: x[i], fields.values())))
        data = unicode('\n'.join(rows))
        print(data)
        form_data = {'type': 'localCSV', 'csv_filename': 'fakefile.csv'}

        with patch('pybossa.importers.csv.io.open', mock_open(read_data= data), create=True):
            for is_private in [True, False]:
                with patch('pybossa.importers.csv.data_access_levels', is_private):
                    [t1, t2, t3, t4, t5, t6] = BulkTaskLocalCSVImport(**form_data).tasks()
                    if is_private:
                        assert_equal(t1['private_fields'], expected_t1_priv_field), t1
                        assert_equal(t2['private_fields'], expected_t2_priv_field), t2
                        assert_equal(t1['info'], expected_t1_field), t1
                        assert_equal(t2['info'], expected_t2_field), t2
                    else:
                        assert_equal(t1['info'], merge_dicts(expected_t1_field, expected_t1_priv_field)), t1
                        assert_equal(t2['info'], merge_dicts(expected_t2_field, expected_t2_priv_field)), t2
                    assert_equal(t1['gold_answers'], expected_t1_gold_ans), t1
                    assert_equal(t2['gold_answers'], expected_t2_gold_ans), t2

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_invalid_typed_fields_import(self, s3_get):
        invalid_fields = {
            'ans1_json': 'not json',
            'ans2_number': 'true',
            'ans3_bool': '7',
            'ans4_null': '6',
            'ans5_gold_json': 'True',
            'ans6_gold_number': 'null',
            'ans7_gold_bool': '5',
            'ans8_gold_null': 'false',
            'ans9_priv_json': "''",
            'ans10_priv_number': '[]',
            'ans11_priv_bool': '{}',
            'ans12_priv_null': '"""a string"""',
            'ans13_priv_gold_json': '"{1,2}"',
            'ans14_priv_gold_number': 'false',
            'ans15_priv_gold_bool': 'null',
            'ans16_priv_gold_null': '7'
        }

        for is_private in [True, False]:
            with patch('pybossa.importers.csv.data_access_levels', is_private):
                for field, value in invalid_fields.items():
                    data = "{}\n{}".format(field, value)
                    with patch('pybossa.importers.csv.io.open', mock_open(read_data= data), create=True):
                        with assert_raises(BulkImportException):
                            [t1] = self.importer.tasks()

    @with_context
    @patch('pybossa.importers.csv.get_import_csv_file')
    def test_correct_field_names(self, s3_get):
        fields = {
            'ans1_json': 'ans1',
            'ans2_number': 'ans2',
            'ans3_bool': 'ans3',
            'ans4_null': 'ans4',
            'ans9_priv_json': "ans9",
            'ans10_priv_number': 'ans10',
            'ans11_priv_bool': 'ans11',
            'ans12_priv_null': 'ans12',
            'data_access': 'data_access',
            'state': None,
            'quorum': None,
            'calibration': None,
            'priority_0': None,
            'n_answers': None,
            'user_pref': None,
            'expiration': None,
            'ans13_priv_gold_json': None,
            'ans14_priv_gold_number': None,
            'ans15_priv_gold_bool': None,
            'ans16_priv_gold_null': None,
            'ans5_gold_json': None,
            'ans6_gold_number': None,
            'ans7_gold_bool': None,
            'ans8_gold_null': None
        }
        form_data = {'type': 'localCSV', 'csv_filename': 'fakefile.csv'}

        for is_private in [True, False]:
            with patch('pybossa.importers.csv.data_access_levels', is_private):
                for header, field_name in fields.items():
                    with patch('pybossa.importers.csv.io.open', mock_open(read_data=header), create=True):
                        field_names = BulkTaskLocalCSVImport(**form_data).fields()
                        assert field_names == ({field_name} if field_name else set()), {'field_names': field_names, 'field_name': field_name}
