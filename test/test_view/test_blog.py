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


import json
from helper import web
from default import db, with_context
from helper.gig_helper import make_subadmin
from factories import ProjectFactory, BlogpostFactory
from mock import patch

from pybossa.repositories import BlogRepository
from pybossa.repositories import UserRepository
blog_repo = BlogRepository(db)
user_repo = UserRepository(db)



class TestBlogpostView(web.Helper):

    @with_context
    @patch('pybossa.view.projects._check_if_redirect_to_password')
    def test_blogposts_get_all(self, redirect):
        """Test blogpost GET all blogposts"""
        redirect.return_value = False
        user = self.create_users()[1]
        project = ProjectFactory.create(owner=user)
        blogpost_1 = BlogpostFactory.create(owner=user, project=project,
                                            title='titleone', published=True)
        blogpost_2 = BlogpostFactory.create(owner=user, project=project,
                                            title='titletwo',
                                            published=True)

        blogpost_3 = BlogpostFactory.create(owner=user, project=project,
                                            title='titlethree',
                                            published=False)

        self.set_proj_passwd_cookie(project)
        url = "/project/%s/blog" % project.short_name

        # As anonymous
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 401, res.status_code

        # As authenticated
        self.register()
        self.signin()
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert 'titleone' in str(res.data)
        assert 'titletwo' in str(res.data)
        assert 'titlethree' not in str(res.data)

    @with_context
    @patch('pybossa.view.projects._check_if_redirect_to_password')
    def test_json_blogposts_get_all(self, redirect):
        """Test JSON blogpost GET all blogposts"""
        redirect.return_value = False
        user = self.create_users()[1]
        project = ProjectFactory.create(owner=user)
        blogpost_1 = BlogpostFactory.create(owner=user, project=project,
                                            title='titleone', published=True)
        blogpost_2 = BlogpostFactory.create(owner=user, project=project,
                                            title='titletwo', published=True)
        blogpost_3 = BlogpostFactory.create(owner=user, project=project,
                                            title='titlethree', published=False)

        url = "/project/%s/blog" % project.short_name

        # As anonymous
        self.set_proj_passwd_cookie(project)
        res = self.app_get_json(url)
        assert res.status_code == 401, res.status_code

        # As authenticated
        self.register()
        self.signin()
        res = self.app_get_json(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        data = json.loads(res.data)
        assert 'api_key' not in data['owner'].keys()
        assert 'email_addr' not in data['owner'].keys()
        assert 'google_user_id' not in data['owner'].keys()
        assert 'facebook_user_id' not in data['owner'].keys()
        assert 'twitter_user_id' not in data['owner'].keys()
        assert len(data['blogposts']) == 2
        for blogpost in data['blogposts']:
            assert blogpost['title'] in ['titleone', 'titletwo']
        self.signout()

        # As owner
        self.signin(email=user.email_addr, password=self.password)
        res = self.app_get_json(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        data = json.loads(res.data)
        assert 'email_addr' in data['owner'].keys()
        assert 'google_user_id' in data['owner'].keys()
        assert 'facebook_user_id' in data['owner'].keys()
        assert 'twitter_user_id' in data['owner'].keys()
        assert len(data['blogposts']) == 3
        for blogpost in data['blogposts']:
            assert blogpost['title'] in ['titleone', 'titletwo', 'titlethree']
        self.signout()



    @with_context
    def test_blogpost_get_all_errors(self):
        """test blogpost get all raises error if the project does not exist"""
        url = "/project/non-existing-project/blog"

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code


    @with_context
    def test_json_blogpost_get_all_errors(self):
        """Test JSON blogpost GET all raises error if the project does not exist"""
        url = "/project/non-existing-project/blog"

        res = self.app_get_json(url, follow_redirects=True)
        data = json.loads(res.data)
        assert res.status_code == 404, res.status_code
        assert data['code'] == 404



    @with_context
    def test_blogpost_get_one(self):
        """Test blogpost GET with id shows one blogpost"""
        user = self.create_users()[1]
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project, title='title',
                                          published=True)
        url = "/project/%s/%s" % (project.short_name, blogpost.id)

        # As anonymous
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 401, res.status_code

        # As authenticated
        self.register()
        self.signin()
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert 'title' in str(res.data)

    @with_context
    def test_blogpost_get_one_draft(self):
        """Test blogpost GET draft with id shows one blogpost"""
        user = self.create_users()[1]
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project, title='title',
                                          published=False)
        url = "/project/%s/%s" % (project.short_name, blogpost.id)

        # As anonymous
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 401, res.status_code

        # As authenticated
        self.register()
        self.signin()
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # As owner
        url = "/project/%s/%s?api_key=%s" % (project.short_name, blogpost.id,
                                             user.api_key)
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert 'title' in str(res.data)


    @with_context
    def test_blogpost_get_one_errors(self):
        """Test blogposts GET non existing posts raises errors"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project1, project2 = ProjectFactory.create_batch(2, owner=user)
        blogpost = BlogpostFactory.create(project=project1)

        # To a non-existing project
        url = "/project/non-existing-project/%s" % blogpost.id
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To a non-existing post
        url = "/project/%s/999999" % project1.short_name
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To an existing post but with a project in the URL it does not belong to
        url = "/project/%s/%s" % (project2.short_name, blogpost.id)
        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code


    from pybossa.view.projects import redirect

    @with_context
    @patch('pybossa.view.projects.redirect', wraps=redirect)
    def test_blogpost_create_by_owner(self, mock_redirect):
        """Test blogposts, project owners can create"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        url = "/project/%s/new-blogpost" % project.short_name

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code

        res = self.app.post(url,
                            data={'title':'blogpost title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 200, res.status_code
        mock_redirect.assert_called_with('/project/%E2%9C%93project1/blog')

        blogpost = blog_repo.get_by(title='blogpost title')
        assert blogpost.title == 'blogpost title', blogpost.title
        assert blogpost.project_id == project.id, blogpost.project.id
        assert blogpost.user_id == user.id, blogpost.user_id
        assert blogpost.published is False, blogpost.published


    @with_context
    def test_blogpost_create_by_anonymous(self):
        """Test blogpost create, anonymous users are redirected to signin"""
        project = ProjectFactory.create()
        url = "/project/%s/new-blogpost" % project.short_name

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert "This feature requires being logged in." in str(res.data), res

        res = self.app.post(url,
                            data={'title':'blogpost title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert "This feature requires being logged in." in str(res.data)

        blogpost = blog_repo.get_by(title='blogpost title')
        assert blogpost == None, blogpost


    @with_context
    def test_blogpost_create_by_non_owner(self):
        """Test blogpost create by non owner of the project is forbidden"""
        self.register()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        url = "/project/%s/new-blogpost" % project.short_name
        self.signout()
        self.register(name='notowner', email='user2@user.com')
        self.signin(email='user2@user.com', password='p4ssw0rd')

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 403, res.status_code

        res = self.app.post(url,
                            data={'title':'blogpost title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 403, res.status_code


    @with_context
    def test_blogpost_create_errors(self):
        """Test blogposts create for non existing projects raises errors"""
        self.register()
        self.signin()
        url = "/project/non-existing-project/new-blogpost"

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        res = self.app.post(url, data={'title':'blogpost title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 404, res.status_code


    @with_context
    @patch('pybossa.view.projects.redirect', wraps=redirect)
    def test_blogpost_update_by_owner(self, mock_redirect):
        """Test blogposts, project owners can update"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project)
        url = "/project/%s/%s/update" % (project.short_name, blogpost.id)

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code

        res = self.app.post(url,
                            data={'id': blogpost.id,
                                  'title':'blogpost title',
                                  'body':'new body',
                                  'published': True},
                            follow_redirects=True)
        assert res.status_code == 200, res.status_code
        mock_redirect.assert_called_with('/project/%E2%9C%93project1/blog')

        blogpost = blog_repo.get_by(title='blogpost title')
        assert blogpost.title == 'blogpost title', blogpost.title
        assert blogpost.body == 'new body', blogpost.body
        assert blogpost.published, blogpost.published


    @with_context
    def test_blogpost_update_by_anonymous(self):
        """Test blogpost update, anonymous users are redirected to signin"""
        project = ProjectFactory.create()
        blogpost = BlogpostFactory.create(project=project, title='title')
        url = "/project/%s/%s/update" % (project.short_name, blogpost.id)

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert "This feature requires being logged in." in str(res.data), res.data

        res = self.app.post(url,
                            data={'id':blogpost.id,
                                  'title':'new title',
                                  'body':'new body'},
                            follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert "This feature requires being logged in." in str(res.data)

        blogpost = blog_repo.get_by()
        assert blogpost.title == 'title', blogpost.title


    @with_context
    def test_blogpost_update_by_non_owner(self):
        """Test blogpost update by non owner of the project is forbidden"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project, title='title', body='body')
        url = "/project/%s/new-blogpost" % project.short_name
        self.signout()
        self.register(name='notowner', email='user2@user.com')
        self.signin(email='user2@user.com', password='p4ssw0rd')
        url = "/project/%s/%s/update" % (project.short_name, blogpost.id)

        res = self.app.get(url, follow_redirects=True)
        assert res.status_code == 403, res.status_code

        res = self.app.post(url,
                            data={'title':'new title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 403, res.status_code

        blogpost = blog_repo.get_by()
        assert blogpost.title == 'title', blogpost.title


    @with_context
    def test_blogpost_update_errors(self):
        """Test blogposts update for non existing projects raises errors"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project1 = ProjectFactory.create(owner=user)
        project2 = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project1, body='body')

        # To a non-existing project
        url = "/project/non-existing-project/%s/update" % blogpost.id
        res = self.app.post(url, data={'title':'new title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To a non-existing post
        url = "/project/%s/999999/update" % project1.short_name
        res = self.app.post(url, data={'title':'new title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To an existing post but with a project in the URL it does not belong to
        url = "/project/%s/%s/update" % (project2.short_name, blogpost.id)
        res = self.app.post(url, data={'title':'new title', 'body':'body'},
                            follow_redirects=True)
        assert res.status_code == 404, res.status_code


    @with_context
    @patch('pybossa.view.projects.redirect', wraps=redirect)
    def test_blogpost_delete_by_owner(self, mock_redirect):
        """Test blogposts, project owner can delete"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project)
        url = "/project/%s/%s/delete" % (project.short_name, blogpost.id)
        redirect_url = '/project/%E2%9C%93project1/blog'

        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        mock_redirect.assert_called_with(redirect_url)

        blogpost = blog_repo.get_by(title='title')
        assert blogpost is None, blogpost



    @with_context
    def test_blogpost_delete_by_anonymous(self):
        """Test blogpost delete, anonymous users are redirected to signin"""
        project = ProjectFactory.create()
        blogpost = BlogpostFactory.create(project=project)
        url = "/project/%s/%s/delete" % (project.short_name, blogpost.id)

        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 200, res.status_code
        assert "This feature requires being logged in." in str(res.data)

        blogpost = blog_repo.get_by()
        assert blogpost is not None


    @with_context
    def test_blogpost_delete_by_non_owner(self):
        """Test blogpost delete by non owner of the project is forbidden"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project)
        url = "/project/%s/new-blogpost" % project.short_name
        self.signout()
        url = "/project/%s/%s/delete" % (project.short_name, blogpost.id)
        self.register(name='notowner', email='user2@user.com')
        self.signin(email='user2@user.com', password='p4ssw0rd')
        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 403, res.status_code

        blogpost = blog_repo.get_by()
        assert blogpost is not None


    @with_context
    def test_blogpost_delete_errors(self):
        """Test blogposts delete for non existing projects raises errors"""
        self.register()
        self.signin()
        user = user_repo.get(1)
        project1 = ProjectFactory.create(owner=user)
        project2 = ProjectFactory.create(owner=user)
        blogpost = BlogpostFactory.create(project=project1)

        # To a non-existing project
        url = "/project/non-existing-project/%s/delete" % blogpost.id
        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To a non-existing post
        url = "/project/%s/999999/delete" % project1.short_name
        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code

        # To an existing post but with a project in the URL it does not belong to
        url = "/project/%s/%s/delete" % (project2.short_name, blogpost.id)
        res = self.app.post(url, follow_redirects=True)
        assert res.status_code == 404, res.status_code
