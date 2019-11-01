import json
from helper import web
from default import with_context
from mock import patch

class TestSubAdmin(web.Helper):

    @with_context
    def test_00_admin_can_access_subadmin_page(self):
        """Test admin accessing subadmin page works"""
        self.register()
        self.signin()
        res = self.app.get('/admin/subadminusers', follow_redirects=True)
        assert "Manage Subadmin Users" in str(res.data), res.data

    @with_context
    def test_01_admin_add_del_subadminuser(self):
        """Test admin add/del user to subadmin group works"""
        self.register()
        self.signout()
        self.register(fullname="Juan Jose", name="juan",
                      email="juan@juan.com", password="juan")
        self.signout()
        # Signin with admin user
        self.signin()
        # Add user.id=1000 (it does not exist)
        res = self.app.get("/admin/users/addsubadmin/1000", follow_redirects=True)
        err = json.loads(res.data)
        assert res.status_code == 404, res.status_code
        assert err['error'] == "User not found", err
        assert err['status_code'] == 404, err

        # Add user.id=2 to admin group
        res = self.app.get("/admin/users/addsubadmin/2", follow_redirects=True)
        assert "Current Users with Subadmin privileges" in str(res.data)
        err_msg = "User.id=2 should be listed as an subadmin"
        assert "Juan Jose" in str(res.data), err_msg

        # Remove user.id=2 from subadmin group
        res = self.app.get("/admin/users/delsubadmin/2", follow_redirects=True)
        assert "Current Users with Subadmin privileges" not in str(res.data)
        err_msg = "User.id=2 should be listed as an subadmin"
        assert "Juan Jose" not in str(res.data), err_msg

        # Delete a non existant user should return an error
        res = self.app.get("/admin/users/delsubadmin/5000", follow_redirects=True)
        err = json.loads(res.data)
        assert res.status_code == 404, res.status_code
        assert err['error'] == "User.id not found", err
        assert err['status_code'] == 404, err

    @with_context
    def test_02_nonadmin_authenticated_user_cannot_add_del_subadmin(self):
        """Test non admin authenticated user cannot add users to subadmin group works"""
        self.register()
        self.signout()
        self.register(fullname="Juan Jose", name="juan",
                      email="juan@juan.com", password="juan")
        self.signout()
        self.register(fullname="Juan Jose2", name="juan2",
                      email="juan2@juan.com", password="juan2")
        self.signout()
        self.signin(email="juan2@juan.com", password="juan2")
        # Add user.id=2 to subadmin group
        res = self.app.get("/admin/users/addsubadmin/2", follow_redirects=True)
        assert res.status == "403 FORBIDDEN",\
            "This action should be forbidden, not enought privileges"
        # Remove user.id=2 from subadmin group
        res = self.app.get("/admin/users/delsubadmin/2", follow_redirects=True)
        assert res.status == "403 FORBIDDEN",\
            "This action should be forbidden, not enought privileges"
