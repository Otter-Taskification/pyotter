from typing import Callable
from PyQt5.QtWidgets import QMainWindow, QMessageBox

from . import layout
from . import dialog


class MainWindow(QMainWindow):

    def __init__(self, quit_action: Callable, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ui = layout.main_window.Ui_OtterMainWindow()
        self.ui.setupUi(self)
        self.setWindowTitle("Otter")
        self.ui.action_quit.triggered.connect(dialog.yes_or_no(self, question="Really quit?", yes=quit_action))
