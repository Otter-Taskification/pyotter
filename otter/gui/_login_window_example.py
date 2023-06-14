from typing import Tuple
from PyQt5 import QtWidgets as qtw
from PyQt5 import QtCore as qtc

from . import layout
from ._login_manager_example import LoginManager


class LoginWindow(qtw.QMainWindow):

    def __init__(self, manager: LoginManager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ui = layout._login_window.Ui_MainWindow()
        self.ui.setupUi(self)
        self.ui.button_login.clicked.connect(self.login)
        self.ui.edit_pass.returnPressed.connect(self.login)
        self.ui.edit_user.returnPressed.connect(self.login)
        self.ui.action_quit.triggered.connect(self.quit)
        self.login_manager = manager

    @property
    def user_pass(self) -> Tuple[str, str]:
        return self.ui.edit_user.text(), self.ui.edit_pass.text()

    @property
    def clear_on_login(self) -> bool:
        return self.ui.check_clear_on_login.checkState() == qtc.Qt.CheckState.Checked

    def clear_user_pass(self) -> None:
        self.ui.edit_user.clear()
        self.ui.edit_pass.clear()

    def login(self) -> None:
        if self.login_manager.authenticate(*self.user_pass):
            qtw.QMessageBox.information(self, "Success", "Logged in")
        else:
            qtw.QMessageBox.critical(self, "Error", "Invalid credentials")
        if self.clear_on_login:
            self.clear_user_pass()

    def quit(self) -> None:
        raise SystemExit(0)
