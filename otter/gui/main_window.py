from PyQt5 import QtWidgets as qtw

from . import ui


class MainWindow(qtw.QMainWindow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ui = ui.main_window.Ui_OtterMainWindow()
        self.ui.setupUi(self)
        # self.setObjectName("This is the name")

    def quit(self) -> None:
        raise SystemExit(0)
