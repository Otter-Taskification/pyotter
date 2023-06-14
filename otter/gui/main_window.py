from PyQt5 import QtWidgets as qtw

from . import layout


class MainWindow(qtw.QMainWindow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ui = layout.main_window.Ui_OtterMainWindow()
        self.ui.setupUi(self)

    def quit(self) -> None:
        raise SystemExit(0)
